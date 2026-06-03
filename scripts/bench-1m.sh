#!/usr/bin/env bash
# Manual 1M-document cold-query benchmark on AWS S3 (Phase A/C, v0.3).
# MinIO is for correctness only; do not use MinIO p50 for latency SLOs.
#
# Usage:
#   ./scripts/bench-1m.sh              # full run (requires AWS env)
#   ./scripts/bench-1m.sh --dry-run    # validate tools + env, no S3/serve
#   OPENPUFFER_BENCH_DRY_RUN=1 ./scripts/bench-1m.sh
#
# Prerequisites: see docs/BENCHMARKS.md § "1M manual (AWS)".
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DRY_RUN=0
for arg in "$@"; do
  case "$arg" in
    --dry-run|-n) DRY_RUN=1 ;;
    -h|--help)
      sed -n '2,12p' "$0"
      exit 0
      ;;
  esac
done
[[ "${OPENPUFFER_BENCH_DRY_RUN:-}" == "1" ]] && DRY_RUN=1

# v0.3: SPFresh v3 index required for 1M gate (warn if overridden).
ANN_VERSION="${OPENPUFFER_ANN_VERSION:-3}"
if [[ "$ANN_VERSION" != "3" ]]; then
  echo "warning: OPENPUFFER_ANN_VERSION=${ANN_VERSION} (v0.3 1M bench expects 3)" >&2
fi
export OPENPUFFER_ANN_VERSION="$ANN_VERSION"

# --- Benchmark tuning ---
NAMESPACE="${OPENPUFFER_BENCH_NAMESPACE:-bench-1m-cold}"
DOCS="${OPENPUFFER_BENCH_DOCS:-1000000}"
DIM="${OPENPUFFER_BENCH_DIM:-128}"
LISTEN="${OPENPUFFER_BENCH_LISTEN:-127.0.0.1:8080}"
RESULTS="${OPENPUFFER_BENCH_RESULTS:-$ROOT/benchmarks/results/1m-aws.json}"
COLD_RUNS="${OPENPUFFER_BENCH_COLD_RUNS:-7}"
RECALL_NUM="${OPENPUFFER_BENCH_RECALL_NUM:-20}"
RECALL_TOP_K="${OPENPUFFER_BENCH_RECALL_TOP_K:-10}"
INDEX_TIMEOUT_SEC="${OPENPUFFER_BENCH_INDEX_TIMEOUT_SEC:-7200}"
ENFORCE_GATES="${OPENPUFFER_BENCH_ENFORCE_GATES:-1}"
INDEX_PREFIX="openpuffer/${NAMESPACE}/index/"

BASE_URL="http://${LISTEN}"
NS_URL="${BASE_URL}/v1/namespaces/${NAMESPACE}"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

validate_toolchain() {
  need_cmd curl
  need_cmd jq
  need_cmd python3
  if [[ "$DRY_RUN" == "0" ]]; then
    need_cmd cargo
  fi
}

validate_aws_env() {
  : "${OPENPUFFER_S3_ENDPOINT:?set OPENPUFFER_S3_ENDPOINT (e.g. https://s3.us-east-1.amazonaws.com)}"
  : "${OPENPUFFER_S3_BUCKET:?set OPENPUFFER_S3_BUCKET}"
  : "${OPENPUFFER_S3_ACCESS_KEY:?set OPENPUFFER_S3_ACCESS_KEY}"
  : "${OPENPUFFER_S3_SECRET_KEY:?set OPENPUFFER_S3_SECRET_KEY}"
}

run_dry_run() {
  validate_toolchain
  echo "bench-1m dry-run OK"
  echo "  OPENPUFFER_ANN_VERSION=${ANN_VERSION} (required: 3 for v0.3)"
  echo "  namespace=${NAMESPACE} docs=${DOCS} dim=${DIM}"
  echo "  listen=${LISTEN} results=${RESULTS}"
  echo "  cold_runs=${COLD_RUNS} recall_num=${RECALL_NUM} index_timeout=${INDEX_TIMEOUT_SEC}s"
  echo "  enforce_gates=${ENFORCE_GATES}"
  if [[ -n "${OPENPUFFER_S3_BUCKET:-}" ]]; then
    echo "  OPENPUFFER_S3_BUCKET=${OPENPUFFER_S3_BUCKET} (set; not contacted in dry-run)"
  else
    echo "  OPENPUFFER_S3_* unset (OK for dry-run; required for full run)"
  fi
  echo "Full run after ingest: export OPENPUFFER_S3_* then ./scripts/bench-1m.sh"
  exit 0
}

build_query_vec() {
  python3 -c "import json; print(json.dumps([(d*0.02).__cos__() for d in range($DIM)]))" 2>/dev/null \
    || python3 -c "import math,json; print(json.dumps([math.cos(d*0.02) for d in range($DIM)]))"
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

# Returns JSON meta on stdout; exits 1 if not ready for cold query.
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
    echo "namespace ${NAMESPACE}: preferred_ann_version=${pref_ann} (expected 3 for v0.3 1M)" >&2
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
  local qvec="$1"
  local t0 ms body roundtrips ratio cold_keys
  t0=$(date +%s%3N)
  body="$(curl -sf -X POST "${BASE_URL}/v2/namespaces/${NAMESPACE}/query" \
    -H 'Content-Type: application/json' \
    -d "$(jq -n \
      --argjson q "$qvec" \
      '{rank_by:["vector","ANN","embedding",$q], top_k:10, consistency:"strong"}')")"
  ms=$(( $(date +%s%3N) - t0 ))
  roundtrips="$(echo "$body" | jq '.performance.storage_roundtrips')"
  ratio="$(echo "$body" | jq '.performance.candidates_ratio')"
  cold_keys="$(echo "$body" | jq '.performance.cold_s3_keys_fetched // 0')"
  echo "${ms} ${roundtrips} ${ratio} ${cold_keys}"
}

p50_ms() {
  local -a sorted=("$@")
  local n=${#sorted[@]}
  local mid=$(( n / 2 ))
  echo "${sorted[$mid]}"
}

count_index_objects_aws() {
  # Optional: list index/ keys when AWS CLI is configured (same filter as bench_cold.rs).
  if [[ -z "${OPENPUFFER_BENCH_SKIP_INDEX_STATS:-}" ]] && command -v aws >/dev/null 2>&1 \
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

validate_toolchain
if [[ "$DRY_RUN" == "1" ]]; then
  run_dry_run
fi

validate_aws_env

echo "Building openpuffer (release)…"
cargo build --release -q

SERVE_PID=""
if [[ -z "${OPENPUFFER_BENCH_SKIP_SERVE:-}" ]]; then
  echo "Starting serve (no cache, ann-version=${ANN_VERSION}) on ${LISTEN}…"
  SERVE_ARGS=(
    serve
    --listen "$LISTEN"
    --cache-dir ""
    --s3-endpoint "$OPENPUFFER_S3_ENDPOINT"
    --s3-bucket "$OPENPUFFER_S3_BUCKET"
    --s3-region "${OPENPUFFER_S3_REGION:-us-east-1}"
    --s3-access-key "$OPENPUFFER_S3_ACCESS_KEY"
    --s3-secret-key "$OPENPUFFER_S3_SECRET_KEY"
    --ann-version "$ANN_VERSION"
  )
  target/release/openpuffer "${SERVE_ARGS[@]}" &
  SERVE_PID=$!
  trap '[[ -n "$SERVE_PID" ]] && kill "$SERVE_PID" 2>/dev/null || true' EXIT
  wait_for_health
else
  wait_for_health
fi

if [[ -z "${OPENPUFFER_BENCH_SKIP_INDEX_WAIT:-}" ]]; then
  echo "Waiting for ${DOCS}-doc namespace ${NAMESPACE} (index_cursor==wal_commit_seq, preferred_ann_version==3, timeout ${INDEX_TIMEOUT_SEC}s)…"
  echo "Ingest is out of band — see docs/BENCHMARKS.md for upsert_columns batching."
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

QUERY_VEC="$(build_query_vec)"
echo "Running ${COLD_RUNS} cold vector queries (cache reset each)…"
LATENCIES=()
LAST_ROUNDTRIPS=""
LAST_RATIO=""
LAST_COLD_KEYS=""
for _ in $(seq 1 "$COLD_RUNS"); do
  reset_cache
  read -r ms roundtrips ratio cold_keys < <(cold_query_once "$QUERY_VEC")
  LATENCIES+=("$ms")
  LAST_ROUNDTRIPS="$roundtrips"
  LAST_RATIO="$ratio"
  LAST_COLD_KEYS="$cold_keys"
done

IFS=$'\n' sorted=($(printf '%s\n' "${LATENCIES[@]}" | sort -n))
P50_MS="$(p50_ms "${sorted[@]}")"

reset_cache
curl -sf -X POST "${BASE_URL}/v2/namespaces/${NAMESPACE}/query" \
  -H 'Content-Type: application/json' \
  -d "$(jq -n --argjson q "$QUERY_VEC" \
    '{rank_by:["vector","ANN","embedding",$q], top_k:10, consistency:"strong"}')" >/dev/null

S3_GET_COUNT="$(curl -sf "${BASE_URL}/v1/debug/cache-stats" | jq '.s3_get_count')"

echo "Measuring recall via POST /v1/namespaces/${NAMESPACE}/recall …"
RECALL_BODY="$(curl -sf -X POST "${BASE_URL}/v1/namespaces/${NAMESPACE}/recall" \
  -H 'Content-Type: application/json' \
  -d "$(jq -n --argjson n "$RECALL_NUM" --argjson k "$RECALL_TOP_K" \
    '{num:$n, top_k:$k, vector_field:"embedding"}')")"
RECALL_AT_10="$(echo "$RECALL_BODY" | jq '.avg_recall')"

mkdir -p "$(dirname "$RESULTS")"

# JSON schema aligned with baseline-10k.json / nightly-100k.json (diffable tiers).
jq -n \
  --arg benchmark "cold_1m" \
  --arg environment "aws-s3" \
  --argjson namespace_docs "$DOCS" \
  --argjson dimensions "$DIM" \
  --arg cache_dir "" \
  --arg consistency "strong" \
  --argjson preferred_ann_version "$PREFERRED_ANN" \
  --argjson index_cursor_eq_wal_commit_seq "$INDEX_CAUGHT_UP" \
  --argjson storage_roundtrips "$LAST_ROUNDTRIPS" \
  --argjson cold_s3_keys_fetched "$LAST_COLD_KEYS" \
  --argjson s3_get_count "$S3_GET_COUNT" \
  --argjson p50_query_latency_ms "$P50_MS" \
  --argjson candidates_ratio "$LAST_RATIO" \
  --argjson recall_at_10 "$RECALL_AT_10" \
  --argjson cold_query_runs "$COLD_RUNS" \
  --arg notes "Manual AWS 1M v0.3 gate. OPENPUFFER_ANN_VERSION=3; meta preferred_ann_version==3 and index_cursor==wal_commit_seq before query. Targets: storage_roundtrips≤4, recall@10≥0.85, p50<600ms. Regenerate: ./scripts/bench-1m.sh" \
  --argjson index_keys_total "${INDEX_KEYS_TOTAL:-null}" \
  --argjson index_object_count "${INDEX_OBJECT_COUNT:-null}" \
  '{
    benchmark: $benchmark,
    environment: $environment,
    namespace_docs: $namespace_docs,
    dimensions: $dimensions,
    cache_dir: $cache_dir,
    consistency: $consistency,
    preferred_ann_version: $preferred_ann_version,
    index_cursor_eq_wal_commit_seq: $index_cursor_eq_wal_commit_seq,
    storage_roundtrips: $storage_roundtrips,
    cold_s3_keys_fetched: $cold_s3_keys_fetched,
    s3_get_count: $s3_get_count,
    s3_get_count_note: "segment cache counter; cold path uses s3_batch (see cold_s3_keys_fetched)",
    p50_query_latency_ms: $p50_query_latency_ms,
    candidates_ratio: $candidates_ratio,
    recall_at_10: $recall_at_10,
    cold_query_runs: $cold_query_runs,
    index_keys_total: $index_keys_total,
    index_object_count: $index_object_count,
    notes: $notes
  }' >"$RESULTS"

echo "Wrote ${RESULTS}"
jq . "$RESULTS"

if [[ "$ENFORCE_GATES" == "1" ]]; then
  jq -e '
    (.preferred_ann_version | tonumber) == 3 and
    .index_cursor_eq_wal_commit_seq == true and
    (.storage_roundtrips | tonumber) <= 4 and
    (.recall_at_10 | tonumber) >= 0.85 and
    (.p50_query_latency_ms | tonumber) < 600
  ' "$RESULTS" >/dev/null || {
    echo "1M gates failed (need preferred_ann_version==3, index caught up, roundtrips≤4, recall@10≥0.85, p50<600ms). Set OPENPUFFER_BENCH_ENFORCE_GATES=0 to record only." >&2
    exit 1
  }
  echo "All 1M gates passed."
fi