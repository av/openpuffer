#!/usr/bin/env bash
# Generator-driven large-tier ingest for openpuffer (Phase 1 / A2).
# Streams upsert batches from benchmarks/workloads/generate_synthetic.py,
# respects manifest cadence (~1.1s between batches), then polls namespace meta
# until index_cursor == wal_commit_seq and preferred_ann_version == 3.
#
# Usage:
#   ./scripts/ingest-large.sh                    # L1 (100k) default
#   ./scripts/ingest-large.sh --tier l3          # 1M docs
#   ./scripts/ingest-large.sh --dry-run          # validate env + print plan
#   OPENPUFFER_INGEST_TIER=l1 ./scripts/ingest-large.sh
#   OPENPUFFER_INGEST_START_BATCH=5 ./scripts/ingest-large.sh  # resume after batch 4 OK
#
# Production S3: transient upsert retries (5xx/429/connection reset) with exponential backoff.
# Env: OPENPUFFER_INGEST_RETRY_MAX (default 6), OPENPUFFER_INGEST_RETRY_BASE_MS (500),
#      OPENPUFFER_INGEST_RETRY_MAX_MS (30000). Failures recorded in ingest JSON sidecar.
#
# After ingest completes, run cold bench:
#   ./scripts/bench-large.sh --tier l1
#
# See docs/PLAN_LARGE_DATASET_BENCHMARK.md (A2) and docs/BENCHMARKS.md § ingest-large sequential batches.
# Upsert batches are strictly sequential (OPENPUFFER_INGEST_PARALLEL must be 0 or unset).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export LARGE_PREFLIGHT_ROOT="$ROOT"
# shellcheck source=scripts/lib/large-benchmark-preflight.sh
source "$ROOT/scripts/lib/large-benchmark-preflight.sh"
# shellcheck source=scripts/lib/ingest-large-retry.sh
source "$ROOT/scripts/lib/ingest-large-retry.sh"
# shellcheck source=scripts/lib/large-benchmark-serve-ready.sh
source "$ROOT/scripts/lib/large-benchmark-serve-ready.sh"

DRY_RUN=0
TIER="${OPENPUFFER_INGEST_TIER:-l1}"
for arg in "$@"; do
  case "$arg" in
    --dry-run|-n) DRY_RUN=1 ;;
    --tier=*) TIER="${arg#*=}" ;;
    --tier) shift; TIER="${1:?--tier requires l1|l2|l3}" ;;
    -h|--help)
      sed -n '2,18p' "$0"
      exit 0
      ;;
  esac
done
[[ "${OPENPUFFER_INGEST_DRY_RUN:-}" == "1" ]] && DRY_RUN=1

ANN_VERSION="${OPENPUFFER_ANN_VERSION:-3}"
if [[ "$ANN_VERSION" != "3" ]]; then
  echo "warning: OPENPUFFER_ANN_VERSION=${ANN_VERSION} (large-tier program expects 3)" >&2
fi
export OPENPUFFER_ANN_VERSION="$ANN_VERSION"

ingest_large_guard_sequential_only() {
  local parallel="${OPENPUFFER_INGEST_PARALLEL:-0}"
  if [[ "$parallel" != "0" ]]; then
    echo "OPENPUFFER_INGEST_PARALLEL=${parallel}: parallel ingest is not implemented." >&2
    echo "  Large-tier ingest uses one upsert batch at a time (WAL commit ordering + index lag observability)." >&2
    echo "  Unset OPENPUFFER_INGEST_PARALLEL or set OPENPUFFER_INGEST_PARALLEL=0." >&2
    echo "  See docs/BENCHMARKS.md § ingest-large sequential batch ingest." >&2
    exit 1
  fi
}
ingest_large_guard_sequential_only

# Tier defaults (wired to synthetic-128 manifests per plan).
case "$TIER" in
  l1) TIER_DOCS=100000; TIER_WORKLOAD="benchmarks/workloads/synthetic-128/l1-100k" ;;
  l2) TIER_DOCS=500000; TIER_WORKLOAD="benchmarks/workloads/synthetic-128/l2-500k" ;;
  l3) TIER_DOCS=1000000; TIER_WORKLOAD="benchmarks/workloads/synthetic-128/l3-1m" ;;
  *)
    echo "unknown tier: ${TIER} (use l1, l2, or l3)" >&2
    exit 1
    ;;
esac

WORKLOAD_DIR="${OPENPUFFER_INGEST_WORKLOAD_DIR:-$TIER_WORKLOAD}"
LISTEN="${OPENPUFFER_INGEST_LISTEN:-127.0.0.1:8080}"
BATCH_SLEEP="${OPENPUFFER_INGEST_BATCH_SLEEP:-}"
INDEX_TIMEOUT_SEC="${OPENPUFFER_INGEST_INDEX_TIMEOUT_SEC:-$(large_preflight_tier_index_timeout_sec "$TIER")}"
DELETE_FIRST="${OPENPUFFER_INGEST_DELETE_FIRST:-0}"
SKIP_SERVE="${OPENPUFFER_INGEST_SKIP_SERVE:-}"
SKIP_UPSERT="${OPENPUFFER_INGEST_SKIP_UPSERT:-}"
BATCH_DIR="${OPENPUFFER_INGEST_BATCH_DIR:-}"
RESULTS="${OPENPUFFER_INGEST_RESULTS:-}"
START_BATCH="${OPENPUFFER_INGEST_START_BATCH:-1}"
INGEST_FAILURE_RECORDS=()
GENERATOR="$ROOT/benchmarks/workloads/generate_synthetic.py"

BASE_URL="http://${LISTEN}"

init_run_context() {
  local mf
  mf="$(manifest_path)"
  resolve_num_docs "$mf"
  NAMESPACE="${OPENPUFFER_INGEST_NAMESPACE:-bench-large-${NUM_DOCS}}"
  V2_NS_URL="${BASE_URL}/v2/namespaces/${NAMESPACE}"
  V1_NS_URL="${BASE_URL}/v1/namespaces/${NAMESPACE}"
  load_manifest_defaults "$mf"
  BATCH_SLEEP_SEC="${BATCH_SLEEP:-$MANIFEST_SLEEP}"
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

validate_s3_env() {
  large_preflight_validate_s3_env
  INGEST_ENVIRONMENT="$(large_preflight_detect_environment)"
  if [[ "$DRY_RUN" == "0" ]]; then
    large_preflight_s3_head_bucket || true
  fi
}

manifest_path() {
  if [[ -f "$WORKLOAD_DIR/manifest.json" ]]; then
    echo "$WORKLOAD_DIR/manifest.json"
    return 0
  fi
  echo ""
}

load_manifest_defaults() {
  local mf="$1"
  MANIFEST_SEED=42
  MANIFEST_DIM=128
  MANIFEST_BATCH_SIZE=10000
  MANIFEST_ID_SCHEME="doc-prefix"
  MANIFEST_EMBEDDING_FN="bench_sin_v1"
  MANIFEST_SLEEP=1.1
  if [[ -z "$mf" || ! -f "$mf" ]]; then
    return 0
  fi
  MANIFEST_SEED="$(jq -r '.seed // 42' "$mf")"
  MANIFEST_DIM="$(jq -r '.dim // 128' "$mf")"
  MANIFEST_BATCH_SIZE="$(jq -r '.batch_size // 10000' "$mf")"
  MANIFEST_ID_SCHEME="$(jq -r '.id_scheme // "doc-prefix"' "$mf")"
  MANIFEST_EMBEDDING_FN="$(jq -r '.embedding_fn // "bench_sin_v1"' "$mf")"
  MANIFEST_SLEEP="$(jq -r '.ingest_cadence.sleep_seconds_between_batches // 1.1' "$mf")"
}

resolve_num_docs() {
  local mf="$1"
  if [[ -n "${OPENPUFFER_INGEST_DOCS:-}" ]]; then
    NUM_DOCS="$OPENPUFFER_INGEST_DOCS"
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

run_dry_run() {
  validate_toolchain
  large_preflight_ann_version
  large_preflight_validate_tier_workload "$TIER" "$ROOT"
  init_run_context
  local mf
  mf="$(manifest_path)"
  local sleep_s="$BATCH_SLEEP_SEC"
  local batches=$(( (NUM_DOCS + MANIFEST_BATCH_SIZE - 1) / MANIFEST_BATCH_SIZE ))
  echo "ingest-large dry-run OK"
  echo "  tier=${TIER} workload_dir=${WORKLOAD_DIR}"
  echo "  namespace=${NAMESPACE} docs=${NUM_DOCS} dim=${MANIFEST_DIM}"
  echo "  batches=${batches} batch_size=${MANIFEST_BATCH_SIZE} sleep=${sleep_s}s"
  echo "  seed=${MANIFEST_SEED} embedding_fn=${MANIFEST_EMBEDDING_FN} id_scheme=${MANIFEST_ID_SCHEME}"
  echo "  listen=${LISTEN} ann_version=${ANN_VERSION}"
  echo "  index_timeout=${INDEX_TIMEOUT_SEC}s delete_first=${DELETE_FIRST}"
  echo "  serve_ready_timeout=$(large_benchmark_serve_ready_timeout_sec)s poll=$(large_benchmark_serve_ready_poll_interval_sec)s"
  echo "  ingest_parallel=0 (sequential batches only)"
  echo "  start_batch=${START_BATCH} retry_max=${OPENPUFFER_INGEST_RETRY_MAX:-6} retry_base_ms=${OPENPUFFER_INGEST_RETRY_BASE_MS:-500}"
  if [[ -n "$mf" ]]; then
    echo "  manifest=${mf}"
  else
    echo "  manifest=(none; generator defaults)"
  fi
  if [[ -n "${OPENPUFFER_S3_BUCKET:-}" ]]; then
    echo "  OPENPUFFER_S3_BUCKET=${OPENPUFFER_S3_BUCKET} (set; not contacted in dry-run)"
  else
    echo "  OPENPUFFER_S3_* unset (OK for dry-run; required for full run)"
  fi
  large_preflight_aws_time_estimate "$TIER"
  exit 0
}

verify_namespace_meta() {
  local meta
  meta="$(curl -sf "${V1_NS_URL}" 2>/dev/null || true)"
  if [[ -z "$meta" ]]; then
    echo "namespace ${NAMESPACE} not found at ${V1_NS_URL}" >&2
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
  local wait_t0
  wait_t0=$(date +%s)
  local deadline=$(( wait_t0 + INDEX_TIMEOUT_SEC ))
  while [[ $(date +%s) -lt $deadline ]]; do
    if verify_namespace_meta >/dev/null 2>&1; then
      local meta cursor pref
      meta="$(verify_namespace_meta)"
      cursor="$(echo "$meta" | jq -r '.index_cursor')"
      pref="$(echo "$meta" | jq -r '.preferred_ann_version // 2')"
      INDEX_WAIT_SEC=$(( $(date +%s) - wait_t0 ))
      echo "namespace ${NAMESPACE} indexed (cursor=${cursor}, preferred_ann_version=${pref}, index_wait=${INDEX_WAIT_SEC}s)"
      return 0
    fi
    sleep 2
  done
  INDEX_WAIT_SEC=$(( $(date +%s) - wait_t0 ))
  echo "timeout waiting for index_cursor == wal_commit_seq and preferred_ann_version==3 on ${NAMESPACE}" >&2
  verify_namespace_meta >&2 || true
  return 1
}

delete_namespace_if_requested() {
  if [[ "$DELETE_FIRST" != "1" ]]; then
    return 0
  fi
  echo "Deleting namespace ${NAMESPACE} (OPENPUFFER_INGEST_DELETE_FIRST=1)…"
  curl -sf -X DELETE "${V2_NS_URL}" >/dev/null 2>&1 || true
}

ensure_batch_dir() {
  if [[ -n "$BATCH_DIR" ]]; then
    if [[ ! -d "$BATCH_DIR/batches" ]]; then
      echo "OPENPUFFER_INGEST_BATCH_DIR=${BATCH_DIR} has no batches/ subdirectory" >&2
      exit 1
    fi
    echo "$BATCH_DIR"
    return 0
  fi

  local tmp
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/openpuffer-ingest-${NAMESPACE}.XXXXXX")"
  echo "Generating ${NUM_DOCS} upsert batches under ${tmp}…" >&2
  python3 "$GENERATOR" \
    --output-dir "$tmp" \
    --num-docs "$NUM_DOCS" \
    --dim "$MANIFEST_DIM" \
    --batch-size "$MANIFEST_BATCH_SIZE" \
    --seed "$MANIFEST_SEED" \
    --id-scheme "$MANIFEST_ID_SCHEME" \
    --embedding-fn "$MANIFEST_EMBEDDING_FN" \
    --write-batches \
    --batch-format openpuffer
  echo "$tmp"
}

validate_start_batch() {
  if [[ ! "$START_BATCH" =~ ^[0-9]+$ ]] || [[ "$START_BATCH" -lt 1 ]]; then
    echo "OPENPUFFER_INGEST_START_BATCH must be a positive integer (got: ${START_BATCH})" >&2
    exit 1
  fi
}

run_ingest_batches() {
  local batch_root="$1"
  local sleep_s="$2"
  local batches_dir="${batch_root}/batches"
  local -a files=()
  local f
  shopt -s nullglob
  for f in "$batches_dir"/batch-*.json; do
    files+=("$f")
  done
  shopt -u nullglob

  if [[ ${#files[@]} -eq 0 ]]; then
    echo "no batch-*.json under ${batches_dir}" >&2
    exit 1
  fi

  validate_start_batch

  local total=${#files[@]}
  INGEST_BATCH_TOTAL="$total"
  if [[ "$START_BATCH" -gt "$total" ]]; then
    echo "OPENPUFFER_INGEST_START_BATCH=${START_BATCH} exceeds batch count ${total}" >&2
    exit 1
  fi

  local i=0
  local t0 t1 batch_ms
  INGEST_BATCH_TIMES_MS=()
  INGEST_BATCH_FILES=()
  INGEST_SKIPPED_BATCHES=0
  t0=$(date +%s)

  for f in "${files[@]}"; do
    i=$((i + 1))
    if [[ "$i" -lt "$START_BATCH" ]]; then
      INGEST_SKIPPED_BATCHES=$((INGEST_SKIPPED_BATCHES + 1))
      continue
    fi

    local bt0 bt1 name
    name="$(basename "$f")"
    bt0=$(date +%s%3N)
    if ! ingest_large_upsert_batch_with_retry "${V2_NS_URL}" "$f" "$i"; then
      local http="${INGEST_LAST_UPSERT_HTTP_CODE:-0}"
      local cerr="${INGEST_LAST_UPSERT_CURL_EXIT:-1}"
      local transient=false
      ingest_large_is_transient_failure "$cerr" "$http" && transient=true
      ingest_large_record_failure "$i" "$name" "${OPENPUFFER_INGEST_RETRY_MAX:-6}" "$http" "$cerr" "$transient" \
        "upsert failed after retries (resume with OPENPUFFER_INGEST_START_BATCH=${i})"
      echo "  batch ${i}/${total} ${name}: FAILED (curl=${cerr} http=${http})" >&2
      echo "  resume: OPENPUFFER_INGEST_START_BATCH=${i} ./scripts/ingest-large.sh --tier ${TIER}" >&2
      write_partial_results_on_failure
      exit 1
    fi
    bt1=$(date +%s%3N)
    batch_ms=$((bt1 - bt0))
    INGEST_BATCH_TIMES_MS+=("$batch_ms")
    INGEST_BATCH_FILES+=("$f")
    echo "  batch ${i}/${total} ${name} ${batch_ms}ms"
    if [[ "$i" -lt "$total" ]]; then
      sleep "$sleep_s"
    fi
  done

  t1=$(date +%s)
  INGEST_WALL_SEC=$((t1 - t0))
  INGEST_BATCH_COUNT="${#INGEST_BATCH_FILES[@]}"
  if [[ "$START_BATCH" -gt 1 ]]; then
    echo "  resumed from batch ${START_BATCH} (skipped ${INGEST_SKIPPED_BATCHES} prior batches)"
  fi
}

build_ingest_batch_runs_json() {
  local -a runs=()
  local i=0 f name
  for f in "${INGEST_BATCH_FILES[@]:-}"; do
    i=$((i + 1))
    name="$(basename "$f")"
    runs+=("$(jq -cn \
      --argjson batch "$i" \
      --arg file "$name" \
      --argjson latency_ms "${INGEST_BATCH_TIMES_MS[$((i - 1))]}" \
      '{batch:$batch, file:$file, latency_ms:$latency_ms}')")
  done
  printf '%s\n' "${runs[@]}" | jq -s '.'
}

compute_batch_latency_percentiles() {
  local pct="$1"
  printf '%s\n' "${INGEST_BATCH_TIMES_MS[@]}" | sort -n | python3 -c "
import sys
vals = [int(x) for x in sys.stdin if x.strip()]
if not vals:
    print(0)
    raise SystemExit
pct = float('$pct')
idx = max(0, min(len(vals) - 1, int((len(vals) * pct + 99) // 100 - 1)))
print(vals[idx])
"
}

write_partial_results_on_failure() {
  local meta=""
  meta="$(curl -sf "${V1_NS_URL}" 2>/dev/null || echo '{}')"
  write_results_json "$meta" "failed"
}

write_results_json() {
  local meta="$1"
  local ingest_status="${2:-ok}"
  local out="${RESULTS:-$ROOT/benchmarks/results/ingest-large-${NUM_DOCS}.json}"
  mkdir -p "$(dirname "$out")"
  local cursor commit pref_ann caught_up
  cursor="$(echo "$meta" | jq -r '.index_cursor // 0')"
  commit="$(echo "$meta" | jq -r '.wal_commit_seq // 0')"
  pref_ann="$(echo "$meta" | jq -r '.preferred_ann_version // 2')"
  caught_up=$([[ "$cursor" == "$commit" && "$commit" != "0" ]] && echo true || echo false)

  local upsert_sec="${INGEST_WALL_SEC:-0}"
  local index_wait_sec="${INDEX_WAIT_SEC:-0}"
  local total_sec=$((upsert_sec + index_wait_sec))
  local batch_count="${INGEST_BATCH_COUNT:-0}"
  local batches_per_sec docs_per_sec
  batches_per_sec="$(python3 -c "b=${batch_count}; u=${upsert_sec}; print(round(b/u, 4) if u>0 and b>0 else 0)")"
  docs_per_sec="$(python3 -c "d=${NUM_DOCS}; u=${upsert_sec}; print(round(d/u, 2) if u>0 else 0)")"

  local batch_p50 batch_p95 batch_min batch_max batch_runs_json
  batch_p50=0
  batch_p95=0
  batch_min=0
  batch_max=0
  batch_runs_json='[]'
  if [[ ${#INGEST_BATCH_TIMES_MS[@]} -gt 0 ]]; then
    batch_p50="$(compute_batch_latency_percentiles 50)"
    batch_p95="$(compute_batch_latency_percentiles 95)"
    batch_min="$(printf '%s\n' "${INGEST_BATCH_TIMES_MS[@]}" | sort -n | head -1)"
    batch_max="$(printf '%s\n' "${INGEST_BATCH_TIMES_MS[@]}" | sort -n | tail -1)"
    batch_runs_json="$(build_ingest_batch_runs_json)"
  fi

  local env_note="${INGEST_ENVIRONMENT:-}"
  [[ -z "$env_note" ]] && env_note="$(large_preflight_detect_environment 2>/dev/null || echo "unknown")"

  local failures_json resume_json
  failures_json="$(ingest_large_failures_json)"
  resume_json="$(jq -cn \
    --argjson start_batch "${START_BATCH:-1}" \
    --argjson skipped_batches "${INGEST_SKIPPED_BATCHES:-0}" \
    --argjson total_batches "${INGEST_BATCH_TOTAL:-$batch_count}" \
    '{start_batch:$start_batch, skipped_batches:$skipped_batches, total_batches:$total_batches}')"

  jq -n \
    --arg benchmark "ingest_large" \
    --arg environment "$env_note" \
    --arg tier "$TIER" \
    --arg workload_dir "$WORKLOAD_DIR" \
    --arg namespace "$NAMESPACE" \
    --argjson num_docs "$NUM_DOCS" \
    --argjson dim "$MANIFEST_DIM" \
    --argjson batch_size "$MANIFEST_BATCH_SIZE" \
    --argjson batch_count "$batch_count" \
    --argjson ingest_elapsed_secs "$upsert_sec" \
    --argjson ingest_wall_sec "$upsert_sec" \
    --argjson index_wait_sec "$index_wait_sec" \
    --argjson ingest_total_wall_sec "$total_sec" \
    --argjson ingest_batches_per_sec "$batches_per_sec" \
    --argjson ingest_docs_per_sec "$docs_per_sec" \
    --argjson batch_sleep_sec "${BATCH_SLEEP_SEC:-$MANIFEST_SLEEP}" \
    --argjson seed "$MANIFEST_SEED" \
    --arg embedding_fn "$MANIFEST_EMBEDDING_FN" \
    --arg id_scheme "$MANIFEST_ID_SCHEME" \
    --argjson preferred_ann_version "$pref_ann" \
    --argjson index_cursor "$cursor" \
    --argjson wal_commit_seq "$commit" \
    --argjson index_ready "$caught_up" \
    --argjson index_timeout_sec "$INDEX_TIMEOUT_SEC" \
    --arg generator "$GENERATOR" \
    --argjson batch_p50_ms "$batch_p50" \
    --argjson batch_p95_ms "$batch_p95" \
    --argjson batch_min_ms "$batch_min" \
    --argjson batch_max_ms "$batch_max" \
    --argjson batch_runs "$batch_runs_json" \
    --argjson ingest_failures "$failures_json" \
    --argjson ingest_resume "$resume_json" \
    --arg ingest_status "$ingest_status" \
    --argjson retry_max "${OPENPUFFER_INGEST_RETRY_MAX:-6}" \
    --argjson retry_base_ms "${OPENPUFFER_INGEST_RETRY_BASE_MS:-500}" \
    --arg notes "A2 ingest-large.sh; upsert cadence from manifest ingest_cadence; index poll until cursor==wal_commit_seq and preferred_ann_version==3" \
    '{
      benchmark: $benchmark,
      environment: $environment,
      tier: $tier,
      workload_dir: $workload_dir,
      namespace: $namespace,
      num_docs: $num_docs,
      dim: $dim,
      batch_size: $batch_size,
      batch_count: $batch_count,
      ingest_elapsed_secs: $ingest_elapsed_secs,
      ingest_wall_sec: $ingest_wall_sec,
      index_wait_sec: $index_wait_sec,
      ingest_total_wall_sec: $ingest_total_wall_sec,
      ingest_batches_per_sec: $ingest_batches_per_sec,
      ingest_docs_per_sec: $ingest_docs_per_sec,
      batch_sleep_sec: $batch_sleep_sec,
      seed: $seed,
      embedding_fn: $embedding_fn,
      id_scheme: $id_scheme,
      preferred_ann_version: $preferred_ann_version,
      index_cursor: $index_cursor,
      wal_commit_seq: $wal_commit_seq,
      index_cursor_eq_wal_commit_seq: $index_ready,
      index_timeout_sec: $index_timeout_sec,
      generator: $generator,
      ingest_status: $ingest_status,
      ingest_failures: $ingest_failures,
      ingest_resume: $ingest_resume,
      ingest_retry: {
        max_attempts: $retry_max,
        base_backoff_ms: $retry_base_ms
      },
      ingest_timing: {
        upsert_wall_sec: $ingest_elapsed_secs,
        index_wait_sec: $index_wait_sec,
        total_wall_sec: $ingest_total_wall_sec,
        batch_count: $batch_count,
        batches_per_sec: $ingest_batches_per_sec,
        docs_per_sec: $ingest_docs_per_sec,
        batch_latency_ms: {
          p50: $batch_p50_ms,
          p95: $batch_p95_ms,
          min: $batch_min_ms,
          max: $batch_max_ms
        },
        batch_runs: $batch_runs,
        ingest_failures: $ingest_failures
      },
      notes: $notes
    }' >"$out"
  echo "Wrote ${out}"
  jq . "$out"
}

validate_toolchain
if [[ "$DRY_RUN" == "1" ]]; then
  run_dry_run
fi

validate_s3_env
large_preflight_validate_tier_workload "$TIER" "$ROOT"

init_run_context
MF="$(manifest_path)"

echo "ingest-large: tier=${TIER} namespace=${NAMESPACE} docs=${NUM_DOCS}"
if [[ -n "$MF" ]]; then
  echo "  manifest=${MF}"
fi

SERVE_PID=""
BATCH_TMP=""
cleanup() {
  if [[ -n "$SERVE_PID" ]]; then
    kill "$SERVE_PID" 2>/dev/null || true
  fi
  if [[ -n "$BATCH_TMP" && -z "${OPENPUFFER_INGEST_BATCH_DIR:-}" ]]; then
    rm -rf "$BATCH_TMP"
  fi
}
trap cleanup EXIT

if [[ -z "$SKIP_SERVE" ]]; then
  echo "Building openpuffer (release)…"
  cargo build --release --features integration -q
  echo "Starting serve (ann-version=${ANN_VERSION}) on ${LISTEN}…"
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
fi
wait_for_health

delete_namespace_if_requested

if [[ -z "$SKIP_UPSERT" ]]; then
  BATCH_TMP="$(ensure_batch_dir)"
  echo "Ingesting ${NUM_DOCS} docs (sleep ${BATCH_SLEEP_SEC}s between batches)…"
  run_ingest_batches "$BATCH_TMP" "$BATCH_SLEEP_SEC"
  echo "Ingest wall time: ${INGEST_WALL_SEC}s (${INGEST_BATCH_COUNT} batches)"
else
  echo "Skipping upsert (OPENPUFFER_INGEST_SKIP_UPSERT=1); polling meta only…"
fi

echo "Waiting for indexer (timeout ${INDEX_TIMEOUT_SEC}s)…"
wait_until_indexed

NS_META="$(verify_namespace_meta)"
write_results_json "$NS_META"

echo "Ingest complete. Next: OPENPUFFER_BENCH_NAMESPACE=${NAMESPACE} ./scripts/bench-large.sh --tier ${TIER}"