#!/usr/bin/env bash
# Validate bench-large JSON schema on MinIO @ 100k (G3 schema only — NOT tpuf/COMPARISON).
# Writes benchmarks/results/large-aws-l1-schema-minio.example.json with environment=minio.
#
# Expects MinIO at OPENPUFFER_TEST_S3_* or compose defaults (see run-integration-s3.sh).
# Ingest ~15–30 min @ L1; gates disabled (MinIO p50 is not an AWS SLO).
#
# Usage:
#   ./scripts/run-minio-large-schema-example.sh
#   ./scripts/run-minio-large-schema-example.sh --tier l1 --skip-ingest  # bench only
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export LARGE_PREFLIGHT_ROOT="$ROOT"
# shellcheck source=scripts/lib/large-benchmark-preflight.sh
source "$ROOT/scripts/lib/large-benchmark-preflight.sh"

TIER="${OPENPUFFER_BENCH_TIER:-l1}"
SKIP_INGEST=0
for arg in "$@"; do
  case "$arg" in
    --tier=*) TIER="${arg#*=}" ;;
    --tier) shift; TIER="${1:?}" ;;
    --skip-ingest) SKIP_INGEST=1 ;;
    -h|--help)
      sed -n '2,12p' "$0"
      exit 0
      ;;
  esac
done

[[ "$TIER" == "l1" ]] || {
  echo "schema example script only supports --tier l1 (100k)" >&2
  exit 1
}

ENDPOINT="${OPENPUFFER_TEST_S3_ENDPOINT:-http://127.0.0.1:9000}"
BUCKET="${OPENPUFFER_TEST_S3_BUCKET:-openpuffer-integration}"
ACCESS="${OPENPUFFER_TEST_S3_ACCESS_KEY:-minioadmin}"
SECRET="${OPENPUFFER_TEST_S3_SECRET_KEY:-minioadmin}"

export OPENPUFFER_S3_ENDPOINT="$ENDPOINT"
export OPENPUFFER_S3_BUCKET="$BUCKET"
export OPENPUFFER_S3_ACCESS_KEY="$ACCESS"
export OPENPUFFER_S3_SECRET_KEY="$SECRET"
export OPENPUFFER_S3_REGION="${OPENPUFFER_S3_REGION:-us-east-1}"
export OPENPUFFER_ANN_VERSION=3
export OPENPUFFER_BENCH_ENVIRONMENT=minio
export OPENPUFFER_BENCH_ALLOW_MINIO_RESULTS=1
export OPENPUFFER_BENCH_ENFORCE_GATES=0
export OPENPUFFER_BENCH_RESULTS="$ROOT/benchmarks/results/large-aws-l1-schema-minio.example.json"
export OPENPUFFER_INGEST_ENVIRONMENT=minio
export OPENPUFFER_INGEST_RESULTS="$ROOT/benchmarks/results/ingest-large-l1-schema-minio.example.json"

large_preflight_toolchain
large_preflight_ann_version
large_preflight_validate_tier_workload "$TIER" "$ROOT"

if ! curl -sf "${ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
  echo "MinIO not reachable at ${ENDPOINT}; start: ./scripts/run-integration-s3.sh" >&2
  exit 1
fi

echo "run-minio-large-schema-example: tier=${TIER} endpoint=${ENDPOINT} bucket=${BUCKET}"
echo "  NOT for COMPARISON.md / tpuf tables — environment=minio schema validation only"

if [[ "$SKIP_INGEST" != "1" ]]; then
  ./scripts/ingest-large.sh --tier "$TIER"
fi

./scripts/bench-large.sh --tier "$TIER"

python3 -c "
import json, sys
p='${OPENPUFFER_BENCH_RESULTS}'
d=json.load(open(p))
assert d['environment']=='minio', d.get('environment')
assert d['benchmark']=='cold_large_l1'
for k in ('tier','workload_dir','recall_at_10','p50_query_latency_ms','storage_roundtrips',
          'preferred_ann_version','index_cursor_eq_wal_commit_seq','cold_query_runs'):
    assert k in d, k
print('schema validation OK:', p)
"

echo "Wrote schema example: ${OPENPUFFER_BENCH_RESULTS}"