#!/usr/bin/env bash
# Manual 1M-document cold-query benchmark on AWS S3 (Phase A/C).
# MinIO is for correctness only; do not use MinIO p50 for latency SLOs.
#
# Prerequisites: see docs/BENCHMARKS.md § "1M manual (AWS)".
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# --- Required (AWS S3) ---
: "${OPENPUFFER_S3_ENDPOINT:?set OPENPUFFER_S3_ENDPOINT (e.g. https://s3.us-east-1.amazonaws.com)}"
: "${OPENPUFFER_S3_BUCKET:?set OPENPUFFER_S3_BUCKET}"
: "${OPENPUFFER_S3_ACCESS_KEY:?set OPENPUFFER_S3_ACCESS_KEY}"
: "${OPENPUFFER_S3_SECRET_KEY:?set OPENPUFFER_S3_SECRET_KEY}"

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

# --- Optional ---
# OPENPUFFER_S3_REGION=us-east-1
# OPENPUFFER_BENCH_SKIP_SERVE=1       # serve already listening on LISTEN
# OPENPUFFER_BENCH_SKIP_INDEX_WAIT=1  # namespace already caught up
# OPENPUFFER_ANN_VERSION=3            # passed to serve if set

BASE_URL="http://${LISTEN}"
NS_URL="${BASE_URL}/v1/namespaces/${NAMESPACE}"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}
need_cmd curl
need_cmd jq

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

wait_until_indexed() {
  local deadline=$(( $(date +%s) + INDEX_TIMEOUT_SEC ))
  while [[ $(date +%s) -lt $deadline ]]; do
    local meta
    meta="$(curl -sf "${NS_URL}" 2>/dev/null || true)"
    if [[ -n "$meta" ]]; then
      local cursor commit
      cursor="$(echo "$meta" | jq -r '.index_cursor // 0')"
      commit="$(echo "$meta" | jq -r '.wal_commit_seq // 0')"
      if [[ "$commit" != "0" && "$cursor" == "$commit" ]]; then
        echo "namespace ${NAMESPACE} indexed (cursor=${cursor})"
        return 0
      fi
    fi
    sleep 2
  done
  echo "timeout waiting for index_cursor == wal_commit_seq on ${NAMESPACE}" >&2
  return 1
}

reset_cache() {
  curl -sf -X POST "${BASE_URL}/v1/debug/cache-stats/reset" >/dev/null
}

cold_query_once() {
  local qvec="$1"
  local t0 ms body roundtrips ratio
  t0=$(date +%s%3N)
  body="$(curl -sf -X POST "${BASE_URL}/v2/namespaces/${NAMESPACE}/query" \
    -H 'Content-Type: application/json' \
    -d "$(jq -n \
      --argjson q "$qvec" \
      '{rank_by:["vector","ANN","embedding",$q], top_k:10, consistency:"strong"}')")"
  ms=$(( $(date +%s%3N) - t0 ))
  roundtrips="$(echo "$body" | jq '.performance.storage_roundtrips')"
  ratio="$(echo "$body" | jq '.performance.candidates_ratio')"
  echo "${ms} ${roundtrips} ${ratio}"
}

p50_ms() {
  local -a sorted=("$@")
  local n=${#sorted[@]}
  local mid=$(( n / 2 ))
  echo "${sorted[$mid]}"
}

echo "Building openpuffer (release)…"
cargo build --release -q

SERVE_PID=""
if [[ -z "${OPENPUFFER_BENCH_SKIP_SERVE:-}" ]]; then
  echo "Starting serve (no cache) on ${LISTEN}…"
  SERVE_ARGS=(
    serve
    --listen "$LISTEN"
    --cache-dir ""
    --s3-endpoint "$OPENPUFFER_S3_ENDPOINT"
    --s3-bucket "$OPENPUFFER_S3_BUCKET"
    --s3-region "${OPENPUFFER_S3_REGION:-us-east-1}"
    --s3-access-key "$OPENPUFFER_S3_ACCESS_KEY"
    --s3-secret-key "$OPENPUFFER_S3_SECRET_KEY"
  )
  if [[ -n "${OPENPUFFER_ANN_VERSION:-}" ]]; then
    SERVE_ARGS+=(--ann-version "$OPENPUFFER_ANN_VERSION")
  fi
  target/release/openpuffer "${SERVE_ARGS[@]}" &
  SERVE_PID=$!
  trap '[[ -n "$SERVE_PID" ]] && kill "$SERVE_PID" 2>/dev/null || true' EXIT
  wait_for_health
else
  wait_for_health
fi

if [[ -z "${OPENPUFFER_BENCH_SKIP_INDEX_WAIT:-}" ]]; then
  echo "Waiting for ${DOCS}-doc namespace ${NAMESPACE} to catch up (timeout ${INDEX_TIMEOUT_SEC}s)…"
  echo "Ingest is out of band — see docs/BENCHMARKS.md for upsert_columns batching."
  wait_until_indexed
else
  echo "Skipping index wait (OPENPUFFER_BENCH_SKIP_INDEX_WAIT=1)"
fi

QUERY_VEC="$(build_query_vec)"
echo "Running ${COLD_RUNS} cold vector queries (cache reset each)…"
LATENCIES=()
LAST_ROUNDTRIPS=""
LAST_RATIO=""
for _ in $(seq 1 "$COLD_RUNS"); do
  reset_cache
  read -r ms roundtrips ratio < <(cold_query_once "$QUERY_VEC")
  LATENCIES+=("$ms")
  LAST_ROUNDTRIPS="$roundtrips"
  LAST_RATIO="$ratio"
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
jq -n \
  --arg benchmark "cold_1m" \
  --arg environment "aws-s3" \
  --arg namespace "$NAMESPACE" \
  --argjson namespace_docs "$DOCS" \
  --argjson dimensions "$DIM" \
  --arg cache_dir "" \
  --arg consistency "strong" \
  --argjson storage_roundtrips "$LAST_ROUNDTRIPS" \
  --argjson s3_get_count "$S3_GET_COUNT" \
  --argjson p50_query_latency_ms "$P50_MS" \
  --argjson candidates_ratio "$LAST_RATIO" \
  --argjson recall_at_10 "$RECALL_AT_10" \
  --argjson cold_query_runs "$COLD_RUNS" \
  --arg notes "Manual AWS 1M gate. Targets: storage_roundtrips≤4, recall@10≥0.85, p50<600ms." \
  '{
    benchmark: $benchmark,
    environment: $environment,
    namespace: $namespace,
    namespace_docs: $namespace_docs,
    dimensions: $dimensions,
    cache_dir: $cache_dir,
    consistency: $consistency,
    index_cursor_eq_wal_commit_seq: true,
    storage_roundtrips: $storage_roundtrips,
    s3_get_count: $s3_get_count,
    p50_query_latency_ms: $p50_query_latency_ms,
    candidates_ratio: $candidates_ratio,
    recall_at_10: $recall_at_10,
    cold_query_runs: $cold_query_runs,
    notes: $notes
  }' >"$RESULTS"

echo "Wrote ${RESULTS}"
jq . "$RESULTS"

if [[ "$ENFORCE_GATES" == "1" ]]; then
  jq -e '
    (.storage_roundtrips | tonumber) <= 4 and
    (.recall_at_10 | tonumber) >= 0.85 and
    (.p50_query_latency_ms | tonumber) < 600
  ' "$RESULTS" >/dev/null || {
    echo "1M gates failed (need roundtrips≤4, recall@10≥0.85, p50<600ms). Set OPENPUFFER_BENCH_ENFORCE_GATES=0 to record only." >&2
    exit 1
  }
  echo "All 1M gates passed."
fi