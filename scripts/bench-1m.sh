#!/usr/bin/env bash
# Manual 1M-document cold-query benchmark on AWS S3 (Phase A/C).
# MinIO is for correctness only; do not use MinIO p50 for latency SLOs.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

: "${OPENPUFFER_S3_ENDPOINT:?set OPENPUFFER_S3_ENDPOINT (AWS S3)}"
: "${OPENPUFFER_S3_BUCKET:?set OPENPUFFER_S3_BUCKET}"
: "${OPENPUFFER_S3_ACCESS_KEY:?set OPENPUFFER_S3_ACCESS_KEY}"
: "${OPENPUFFER_S3_SECRET_KEY:?set OPENPUFFER_S3_SECRET_KEY}"

NAMESPACE="${OPENPUFFER_BENCH_NAMESPACE:-bench-1m-cold}"
DOCS="${OPENPUFFER_BENCH_DOCS:-1000000}"
DIM="${OPENPUFFER_BENCH_DIM:-128}"
LISTEN="${OPENPUFFER_BENCH_LISTEN:-127.0.0.1:8080}"
RESULTS="${OPENPUFFER_BENCH_RESULTS:-$ROOT/benchmarks/results/1m-aws.json}"

echo "Building openpuffer (release)…"
cargo build --release -q

echo "Starting serve (no cache) on $LISTEN…"
target/release/openpuffer serve \
  --listen "$LISTEN" \
  --cache-dir "" \
  --s3-endpoint "$OPENPUFFER_S3_ENDPOINT" \
  --s3-bucket "$OPENPUFFER_S3_BUCKET" \
  --s3-region "${OPENPUFFER_S3_REGION:-us-east-1}" \
  --s3-access-key "$OPENPUFFER_S3_ACCESS_KEY" \
  --s3-secret-key "$OPENPUFFER_S3_SECRET_KEY" &
SERVE_PID=$!
trap 'kill "$SERVE_PID" 2>/dev/null || true' EXIT

for _ in $(seq 1 120); do
  if curl -sf "http://${LISTEN}/health" >/dev/null; then
    break
  fi
  sleep 0.5
done

echo "Ingest $DOCS docs (upsert_columns batches) — see docs/BENCHMARKS.md for batching cadence."
echo "After index_cursor == wal_commit_seq, run cold vector query and capture performance JSON."
echo "Write results to: $RESULTS"
echo ""
echo "Example query (after ingest + index catch-up):"
echo "  curl -s -X POST \"http://${LISTEN}/v2/namespaces/${NAMESPACE}/query\" \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"rank_by\":[\"vector\",\"ANN\",\"embedding\",<128-dim query>],\"top_k\":10,\"consistency\":\"strong\"}'"
echo ""
echo "Targets (Phase A/C): storage_roundtrips ≤ 4, recall@10 ≥ 0.85, p50 < 600ms on AWS."