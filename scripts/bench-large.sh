#!/usr/bin/env bash
# Tiered large-dataset cold-query benchmark on AWS S3 (Phase 1 / A3).
# Uses workload queries.json (shared with ingest-large / tpuf driver).
#
# Usage:
#   ./scripts/bench-large.sh                 # L1 (100k) after ingest-large
#   ./scripts/bench-large.sh --tier l3       # 1M namespace
#   ./scripts/bench-large.sh --dry-run       # validate tools + env, no S3/serve
#   OPENPUFFER_BENCH_TIER=l1 ./scripts/bench-large.sh
#
# Prerequisites: ingest via ./scripts/ingest-large.sh; see docs/PLAN_LARGE_DATASET_BENCHMARK.md.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export LARGE_PREFLIGHT_ROOT="$ROOT"
# shellcheck source=scripts/lib/large-benchmark-preflight.sh
source "$ROOT/scripts/lib/large-benchmark-preflight.sh"

DRY_RUN=0
TIER="${OPENPUFFER_BENCH_TIER:-l1}"
for arg in "$@"; do
  case "$arg" in
    --dry-run|-n) DRY_RUN=1 ;;
    --tier=*) TIER="${arg#*=}" ;;
    --tier) shift; TIER="${1:?--tier requires l1|l2|l3}" ;;
    -h|--help)
      sed -n '2,14p' "$0"
      exit 0
      ;;
  esac
done
[[ "${OPENPUFFER_BENCH_DRY_RUN:-}" == "1" ]] && DRY_RUN=1

ANN_VERSION="${OPENPUFFER_ANN_VERSION:-3}"
if [[ "$ANN_VERSION" != "3" ]]; then
  echo "warning: OPENPUFFER_ANN_VERSION=${ANN_VERSION} (large-tier program expects 3)" >&2
fi
export OPENPUFFER_ANN_VERSION="$ANN_VERSION"

case "$TIER" in
  l1) TIER_DOCS=100000; TIER_WORKLOAD="benchmarks/workloads/synthetic-128/l1-100k" ;;
  l2) TIER_DOCS=500000; TIER_WORKLOAD="benchmarks/workloads/synthetic-128/l2-500k" ;;
  l3) TIER_DOCS=1000000; TIER_WORKLOAD="benchmarks/workloads/synthetic-128/l3-1m" ;;
  *)
    echo "unknown tier: ${TIER} (use l1, l2, or l3)" >&2
    exit 1
    ;;
esac

WORKLOAD_DIR="${OPENPUFFER_BENCH_WORKLOAD_DIR:-$TIER_WORKLOAD}"
LISTEN="${OPENPUFFER_BENCH_LISTEN:-127.0.0.1:8080}"
RESULTS="${OPENPUFFER_BENCH_RESULTS:-$ROOT/benchmarks/results/large-aws-${TIER}.json}"
INDEX_TIMEOUT_SEC="${OPENPUFFER_BENCH_INDEX_TIMEOUT_SEC:-7200}"
ENFORCE_GATES="${OPENPUFFER_BENCH_ENFORCE_GATES:-1}"
SKIP_SERVE="${OPENPUFFER_BENCH_SKIP_SERVE:-}"
SKIP_INDEX_WAIT="${OPENPUFFER_BENCH_SKIP_INDEX_WAIT:-}"
SKIP_INDEX_STATS="${OPENPUFFER_BENCH_SKIP_INDEX_STATS:-}"

BASE_URL="http://${LISTEN}"

init_run_context() {
  local mf qf
  mf="$(manifest_path)"
  qf="$(queries_path)"
  resolve_num_docs "$mf"
  NAMESPACE="${OPENPUFFER_BENCH_NAMESPACE:-bench-large-${NUM_DOCS}}"
  DOCS="$NUM_DOCS"
  NS_URL="${BASE_URL}/v1/namespaces/${NAMESPACE}"
  QUERY_URL="${BASE_URL}/v2/namespaces/${NAMESPACE}/query"
  INDEX_PREFIX="openpuffer/${NAMESPACE}/index/"
  load_manifest_defaults "$mf"
  load_query_protocol "$qf"
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

validate_toolchain() {
  large_preflight_toolchain
  if [[ "$DRY_RUN" == "0" && -z "$SKIP_SERVE" ]]; then
    need_cmd cargo
  fi
}

validate_aws_env() {
  large_preflight_validate_s3_env
  BENCH_ENVIRONMENT="$(large_preflight_detect_environment)"
  if [[ "$DRY_RUN" == "0" ]]; then
    large_preflight_s3_head_bucket || true
    large_preflight_guard_aws_results_path "$BENCH_ENVIRONMENT" "$RESULTS"
  fi
}

manifest_path() {
  if [[ -f "$WORKLOAD_DIR/manifest.json" ]]; then
    echo "$WORKLOAD_DIR/manifest.json"
    return 0
  fi
  echo ""
}

queries_path() {
  if [[ -f "$WORKLOAD_DIR/queries.json" ]]; then
    echo "$WORKLOAD_DIR/queries.json"
    return 0
  fi
  echo ""
}

load_manifest_defaults() {
  local mf="$1"
  MANIFEST_SEED=42
  MANIFEST_DIM=128
  MANIFEST_EMBEDDING_FN="bench_sin_v1"
  if [[ -z "$mf" || ! -f "$mf" ]]; then
    return 0
  fi
  MANIFEST_SEED="$(jq -r '.seed // 42' "$mf")"
  MANIFEST_DIM="$(jq -r '.dim // 128' "$mf")"
  MANIFEST_EMBEDDING_FN="$(jq -r '.embedding_fn // "bench_sin_v1"' "$mf")"
}

resolve_num_docs() {
  local mf="$1"
  if [[ -n "${OPENPUFFER_BENCH_DOCS:-}" ]]; then
    NUM_DOCS="$OPENPUFFER_BENCH_DOCS"
    return 0
  fi
  if [[ -n "$mf" && -f "$mf" ]]; then
    NUM_DOCS="$(jq -r '.num_docs // empty' "$mf")"
    if [[ -n "$NUM_DOCS" && "$NUM_DOCS" != "null" ]]; then
      return 0
    fi
  fi
  NUM_DOCS="$TIER_DOCS"
}

load_query_protocol() {
  local qf="$1"
  DIM="${OPENPUFFER_BENCH_DIM:-$MANIFEST_DIM}"
  COLD_RUNS="${OPENPUFFER_BENCH_COLD_RUNS:-7}"
  RECALL_NUM="${OPENPUFFER_BENCH_RECALL_NUM:-20}"
  RECALL_TOP_K="${OPENPUFFER_BENCH_RECALL_TOP_K:-10}"
  QUERY_TOP_K=10
  QUERY_CONSISTENCY="strong"
  PRIMARY_QUERY_NAME="vector-q00"
  QUERY_VEC=""

  if [[ -n "$qf" && -f "$qf" ]]; then
    COLD_RUNS="${OPENPUFFER_BENCH_COLD_RUNS:-$(jq -r '.cold_query_protocol.runs // 7' "$qf")}"
    QUERY_TOP_K="$(jq -r '.cold_query_protocol.top_k // 10' "$qf")"
    QUERY_CONSISTENCY="$(jq -r '.cold_query_protocol.consistency // "strong"' "$qf")"
    PRIMARY_QUERY_NAME="$(jq -r '.vector_queries[0].name // "vector-q00"' "$qf")"
    QUERY_VEC="$(jq -c '.vector_queries[0].vector' "$qf")"
    RECALL_NUM="${OPENPUFFER_BENCH_RECALL_NUM:-$(jq -r '.recall_defaults.num // 20' "$qf")}"
    RECALL_TOP_K="${OPENPUFFER_BENCH_RECALL_TOP_K:-$(jq -r '.recall_defaults.top_k // 10' "$qf")}"
    QUERIES_JSON="$qf"
  else
    echo "warning: no queries.json under ${WORKLOAD_DIR}; using bench_sin_v1 fallback vector" >&2
    QUERIES_JSON=""
    QUERY_VEC="$(build_fallback_query_vec)"
  fi

  QUERY_BODY="$(jq -cn \
    --argjson q "$QUERY_VEC" \
    --argjson top_k "$QUERY_TOP_K" \
    --arg consistency "$QUERY_CONSISTENCY" \
    '{rank_by:["vector","ANN","embedding",$q], top_k:$top_k, consistency:$consistency}')"
}

build_fallback_query_vec() {
  python3 -c "import json; print(json.dumps([(d*0.02).__cos__() for d in range($DIM)]))" 2>/dev/null \
    || python3 -c "import math,json; print(json.dumps([math.cos(d*0.02) for d in range($DIM)]))"
}

run_dry_run() {
  validate_toolchain
  large_preflight_ann_version
  large_preflight_validate_tier_workload "$TIER" "$ROOT"
  init_run_context
  local mf qf
  mf="$(manifest_path)"
  qf="$(queries_path)"
  echo "bench-large dry-run OK"
  echo "  tier=${TIER} workload_dir=${WORKLOAD_DIR}"
  echo "  namespace=${NAMESPACE} docs=${DOCS} dim=${DIM}"
  echo "  listen=${LISTEN} results=${RESULTS}"
  echo "  cold_runs=${COLD_RUNS} primary_query=${PRIMARY_QUERY_NAME}"
  echo "  recall_num=${RECALL_NUM} index_timeout=${INDEX_TIMEOUT_SEC}s"
  echo "  enforce_gates=${ENFORCE_GATES} ann_version=${ANN_VERSION}"
  if [[ -n "$qf" ]]; then
    echo "  queries=${qf}"
  else
    echo "  queries=(fallback sin vector)"
  fi
  if [[ -n "$mf" ]]; then
    echo "  manifest=${mf}"
  fi
  if [[ -n "${OPENPUFFER_S3_BUCKET:-}" ]]; then
    echo "  OPENPUFFER_S3_BUCKET=${OPENPUFFER_S3_BUCKET} (set; not contacted in dry-run)"
  else
    echo "  OPENPUFFER_S3_* unset (OK for dry-run; required for full run)"
  fi
  echo "Full run after ingest: export OPENPUFFER_S3_* then ./scripts/bench-large.sh --tier ${TIER}"
  exit 0
}

wait_for_health() {
  for _ in $(seq 1 120); do
    if curl -sf "${BASE_URL}/health" >/dev/null; then
      return 0
    fi
    sleep 0.5
  done
  echo "serve did not become healthy on ${BASE_URL}/health" >&2
  return 1
}

verify_namespace_meta() {
  local meta
  meta="$(curl -sf "${NS_URL}" 2>/dev/null || true)"
  if [[ -z "$meta" ]]; then
    echo "namespace ${NAMESPACE} not found at ${NS_URL}" >&2
    return 1
  fi

  local cursor commit pref_ann
  cursor="$(echo "$meta" | jq -r '.index_cursor // 0')"
  commit="$(echo "$meta" | jq -r '.wal_commit_seq // 0')"
  pref_ann="$(echo "$meta" | jq -r '.preferred_ann_version // 2')"

  if [[ "$commit" == "0" ]]; then
    echo "namespace ${NAMESPACE}: wal_commit_seq is 0 (no ingest?)" >&2
    return 1
  fi
  if [[ "$cursor" != "$commit" ]]; then
    echo "namespace ${NAMESPACE}: index_cursor=${cursor} != wal_commit_seq=${commit}" >&2
    return 1
  fi
  if [[ "$pref_ann" != "3" ]]; then
    echo "namespace ${NAMESPACE}: preferred_ann_version=${pref_ann} (expected 3)" >&2
    return 1
  fi

  echo "$meta"
  return 0
}

wait_until_indexed() {
  local deadline=$(( $(date +%s) + INDEX_TIMEOUT_SEC ))
  while [[ $(date +%s) -lt $deadline ]]; do
    if verify_namespace_meta >/dev/null 2>&1; then
      local meta cursor pref
      meta="$(verify_namespace_meta)"
      cursor="$(echo "$meta" | jq -r '.index_cursor')"
      pref="$(echo "$meta" | jq -r '.preferred_ann_version // 2')"
      echo "namespace ${NAMESPACE} ready (cursor=${cursor}, preferred_ann_version=${pref})"
      return 0
    fi
    sleep 2
  done
  echo "timeout waiting for index_cursor == wal_commit_seq and preferred_ann_version==3 on ${NAMESPACE}" >&2
  verify_namespace_meta >&2 || true
  return 1
}

reset_cache() {
  curl -sf -X POST "${BASE_URL}/v1/debug/cache-stats/reset" >/dev/null
}

cold_query_once() {
  local run_idx="$1"
  local t0 ms body roundtrips ratio cold_keys
  t0=$(date +%s%3N)
  body="$(curl -sf -X POST "${QUERY_URL}" \
    -H 'Content-Type: application/json' \
    -d "$QUERY_BODY")"
  ms=$(( $(date +%s%3N) - t0 ))
  roundtrips="$(echo "$body" | jq '.performance.storage_roundtrips')"
  ratio="$(echo "$body" | jq '.performance.candidates_ratio')"
  cold_keys="$(echo "$body" | jq '.performance.cold_s3_keys_fetched // 0')"
  echo "${ms} ${roundtrips} ${ratio} ${cold_keys}"
}

percentile_ms() {
  local pct="$1"
  shift
  local -a sorted=("$@")
  local n=${#sorted[@]}
  if [[ "$n" -eq 0 ]]; then
    echo 0
    return 0
  fi
  local idx=$(( (n * pct + 99) / 100 - 1 ))
  if [[ "$idx" -lt 0 ]]; then
    idx=0
  fi
  if [[ "$idx" -ge "$n" ]]; then
    idx=$(( n - 1 ))
  fi
  echo "${sorted[$idx]}"
}

count_index_objects_aws() {
  if [[ -z "$SKIP_INDEX_STATS" ]] && command -v aws >/dev/null 2>&1 \
    && [[ -n "${OPENPUFFER_S3_BUCKET:-}" ]]; then
    local region="${OPENPUFFER_S3_REGION:-us-east-1}"
    local keys_json total ann_count
    keys_json="$(aws s3api list-objects-v2 \
      --bucket "$OPENPUFFER_S3_BUCKET" \
      --prefix "$INDEX_PREFIX" \
      --region "$region" \
      --output json 2>/dev/null || echo '{}')"
    total="$(echo "$keys_json" | jq '[.Contents[]?.Key // empty] | length')"
    ann_count="$(echo "$keys_json" | jq '
      [.Contents[]?.Key // empty]
      | map(select(test("clusters-") or (test("centroids-l1-") and test("\\.bin$"))))
      | length')"
    echo "${total:-0} ${ann_count:-0}"
    return 0
  fi
  echo "null null"
}

tier_recall_gate() {
  case "$TIER" in
    l1) echo "0.85" ;;
    l2) echo "0.85" ;;
    l3) echo "0.85" ;;
    *) echo "0.85" ;;
  esac
}

validate_toolchain
if [[ "$DRY_RUN" == "1" ]]; then
  run_dry_run
fi

validate_aws_env
large_preflight_validate_tier_workload "$TIER" "$ROOT"
large_preflight_ann_version
init_run_context
BENCH_ENVIRONMENT="${OPENPUFFER_BENCH_ENVIRONMENT:-$(large_preflight_detect_environment)}"

echo "bench-large: tier=${TIER} namespace=${NAMESPACE} docs=${DOCS}"
echo "  workload=${WORKLOAD_DIR} query=${PRIMARY_QUERY_NAME}"

SERVE_PID=""
cleanup() {
  [[ -n "$SERVE_PID" ]] && kill "$SERVE_PID" 2>/dev/null || true
}
trap cleanup EXIT

if [[ -z "$SKIP_SERVE" ]]; then
  echo "Building openpuffer (release)…"
  cargo build --release -q
  echo "Starting serve (no cache, ann-version=${ANN_VERSION}) on ${LISTEN}…"
  target/release/openpuffer serve \
    --listen "$LISTEN" \
    --cache-dir "" \
    --s3-endpoint "$OPENPUFFER_S3_ENDPOINT" \
    --s3-bucket "$OPENPUFFER_S3_BUCKET" \
    --s3-region "${OPENPUFFER_S3_REGION:-us-east-1}" \
    --s3-access-key "$OPENPUFFER_S3_ACCESS_KEY" \
    --s3-secret-key "$OPENPUFFER_S3_SECRET_KEY" \
    --ann-version "$ANN_VERSION" &
  SERVE_PID=$!
  wait_for_health
else
  wait_for_health
fi

if [[ -z "$SKIP_INDEX_WAIT" ]]; then
  echo "Waiting for ${DOCS}-doc namespace ${NAMESPACE} (index_cursor==wal_commit_seq, preferred_ann_version==3, timeout ${INDEX_TIMEOUT_SEC}s)…"
  wait_until_indexed
else
  echo "Skipping index wait (OPENPUFFER_BENCH_SKIP_INDEX_WAIT=1); verifying meta…"
  verify_namespace_meta >/dev/null
fi

NS_META="$(verify_namespace_meta)"
PREFERRED_ANN="$(echo "$NS_META" | jq -r '.preferred_ann_version // 2')"
INDEX_CURSOR="$(echo "$NS_META" | jq -r '.index_cursor // 0')"
WAL_COMMIT="$(echo "$NS_META" | jq -r '.wal_commit_seq // 0')"
INDEX_CAUGHT_UP=$([[ "$INDEX_CURSOR" == "$WAL_COMMIT" && "$WAL_COMMIT" != "0" ]] && echo true || echo false)

read -r INDEX_KEYS_TOTAL INDEX_OBJECT_COUNT < <(count_index_objects_aws)

echo "Running ${COLD_RUNS} cold vector queries (${PRIMARY_QUERY_NAME}, cache reset each)…"
LATENCIES=()
RUN_LINES=()
LAST_ROUNDTRIPS=""
LAST_RATIO=""
LAST_COLD_KEYS=""
run_i=0
for _ in $(seq 1 "$COLD_RUNS"); do
  run_i=$((run_i + 1))
  reset_cache
  read -r ms roundtrips ratio cold_keys < <(cold_query_once "$run_i")
  LATENCIES+=("$ms")
  RUN_LINES+=("$(jq -cn \
    --argjson run "$run_i" \
    --argjson latency_ms "$ms" \
    --argjson storage_roundtrips "$roundtrips" \
    --argjson candidates_ratio "$ratio" \
    --argjson cold_s3_keys_fetched "$cold_keys" \
    --arg query_name "$PRIMARY_QUERY_NAME" \
    '{run:$run, query_name:$query_name, latency_ms:$latency_ms, storage_roundtrips:$storage_roundtrips, candidates_ratio:$candidates_ratio, cold_s3_keys_fetched:$cold_s3_keys_fetched}')")
  LAST_ROUNDTRIPS="$roundtrips"
  LAST_RATIO="$ratio"
  LAST_COLD_KEYS="$cold_keys"
done

IFS=$'\n' sorted=($(printf '%s\n' "${LATENCIES[@]}" | sort -n))
P50_MS="$(percentile_ms 50 "${sorted[@]}")"
P95_MS="$(percentile_ms 95 "${sorted[@]}")"

reset_cache
curl -sf -X POST "${QUERY_URL}" \
  -H 'Content-Type: application/json' \
  -d "$QUERY_BODY" >/dev/null

S3_GET_COUNT="$(curl -sf "${BASE_URL}/v1/debug/cache-stats" | jq '.s3_get_count')"

echo "Measuring recall via POST /v1/namespaces/${NAMESPACE}/recall …"
RECALL_BODY="$(curl -sf -X POST "${BASE_URL}/v1/namespaces/${NAMESPACE}/recall" \
  -H 'Content-Type: application/json' \
  -d "$(jq -n --argjson n "$RECALL_NUM" --argjson k "$RECALL_TOP_K" \
    '{num:$n, top_k:$k, vector_field:"embedding"}')")"
RECALL_AT_10="$(echo "$RECALL_BODY" | jq '.avg_recall')"

COLD_RUNS_JSON="$(printf '%s\n' "${RUN_LINES[@]}" | jq -s '.')"
RECALL_GATE="$(tier_recall_gate)"
BENCHMARK_NAME="cold_large_${TIER}"

mkdir -p "$(dirname "$RESULTS")"

HOST_NOTE=""
if [[ -n "${OPENPUFFER_BENCH_HOST_LABEL:-}" ]]; then
  HOST_NOTE=" host=${OPENPUFFER_BENCH_HOST_LABEL}"
fi
if [[ -n "${OPENPUFFER_BENCH_CLIENT_MODE:-}" ]]; then
  HOST_NOTE="${HOST_NOTE} client_mode=${OPENPUFFER_BENCH_CLIENT_MODE}"
fi

jq -n \
  --arg benchmark "$BENCHMARK_NAME" \
  --arg environment "$BENCH_ENVIRONMENT" \
  --arg tier "$TIER" \
  --arg workload_dir "$WORKLOAD_DIR" \
  --arg namespace "$NAMESPACE" \
  --arg primary_query "$PRIMARY_QUERY_NAME" \
  --argjson namespace_docs "$DOCS" \
  --argjson dimensions "$DIM" \
  --argjson seed "$MANIFEST_SEED" \
  --arg embedding_fn "$MANIFEST_EMBEDDING_FN" \
  --arg cache_dir "" \
  --arg consistency "$QUERY_CONSISTENCY" \
  --argjson preferred_ann_version "$PREFERRED_ANN" \
  --argjson index_cursor_eq_wal_commit_seq "$INDEX_CAUGHT_UP" \
  --argjson storage_roundtrips "$LAST_ROUNDTRIPS" \
  --argjson cold_s3_keys_fetched "$LAST_COLD_KEYS" \
  --argjson s3_get_count "$S3_GET_COUNT" \
  --argjson p50_query_latency_ms "$P50_MS" \
  --argjson p95_query_latency_ms "$P95_MS" \
  --argjson candidates_ratio "$LAST_RATIO" \
  --argjson recall_at_10 "$RECALL_AT_10" \
  --argjson cold_query_runs "$COLD_RUNS" \
  --argjson cold_runs "$COLD_RUNS_JSON" \
  --arg notes "A3 bench-large.sh tier=${TIER}; environment=${BENCH_ENVIRONMENT}; workload queries.json; OPENPUFFER_ANN_VERSION=3. Targets (AWS): storage_roundtrips≤4, recall@10≥${RECALL_GATE}, p50<600ms.${HOST_NOTE} Regenerate: ./scripts/bench-large.sh --tier ${TIER}" \
  --argjson index_keys_total "${INDEX_KEYS_TOTAL:-null}" \
  --argjson index_object_count "${INDEX_OBJECT_COUNT:-null}" \
  '{
    benchmark: $benchmark,
    environment: $environment,
    tier: $tier,
    workload_dir: $workload_dir,
    namespace: $namespace,
    primary_query: $primary_query,
    namespace_docs: $namespace_docs,
    dimensions: $dimensions,
    seed: $seed,
    embedding_fn: $embedding_fn,
    cache_dir: $cache_dir,
    consistency: $consistency,
    preferred_ann_version: $preferred_ann_version,
    index_cursor_eq_wal_commit_seq: $index_cursor_eq_wal_commit_seq,
    storage_roundtrips: $storage_roundtrips,
    cold_s3_keys_fetched: $cold_s3_keys_fetched,
    s3_get_count: $s3_get_count,
    s3_get_count_note: "segment cache counter; cold path uses s3_batch (see cold_s3_keys_fetched)",
    p50_query_latency_ms: $p50_query_latency_ms,
    p95_query_latency_ms: $p95_query_latency_ms,
    candidates_ratio: $candidates_ratio,
    recall_at_10: $recall_at_10,
    cold_query_runs: $cold_query_runs,
    cold_runs: $cold_runs,
    index_keys_total: $index_keys_total,
    index_object_count: $index_object_count,
    notes: $notes
  }' >"$RESULTS"

echo "Wrote ${RESULTS}"
jq . "$RESULTS"

if [[ "$ENFORCE_GATES" == "1" && "$BENCH_ENVIRONMENT" == "aws-s3" ]]; then
  jq -e \
    --argjson recall_gate "$RECALL_GATE" \
    '
    (.preferred_ann_version | tonumber) == 3 and
    .index_cursor_eq_wal_commit_seq == true and
    (.storage_roundtrips | tonumber) <= 4 and
    (.recall_at_10 | tonumber) >= ($recall_gate | tonumber) and
    (.p50_query_latency_ms | tonumber) < 600
  ' "$RESULTS" >/dev/null || {
    echo "large-tier gates failed (need preferred_ann_version==3, index caught up, roundtrips≤4, recall@10≥${RECALL_GATE}, p50<600ms). Set OPENPUFFER_BENCH_ENFORCE_GATES=0 to record only." >&2
    exit 1
  }
  echo "All large-tier gates passed (tier=${TIER})."
elif [[ "$ENFORCE_GATES" == "1" && "$BENCH_ENVIRONMENT" != "aws-s3" ]]; then
  echo "Skipping AWS p50 SLO gates (environment=${BENCH_ENVIRONMENT}); set OPENPUFFER_BENCH_ENFORCE_GATES=0 to silence."
fi