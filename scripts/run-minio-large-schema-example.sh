#!/usr/bin/env bash
# Validate bench-large JSON schema on MinIO (G3 schema only — NOT tpuf/COMPARISON).
# Default: L1 @ 100k → large-aws-l1-schema-minio.example.json (environment=minio).
#
# Expects MinIO at OPENPUFFER_TEST_S3_* or compose defaults (see run-integration-s3.sh).
# Ingest ~15–30 min @ 100k; gates disabled (MinIO p50 is not an AWS SLO).
#
# Usage:
#   ./scripts/run-minio-large-schema-example.sh
#   ./scripts/run-minio-large-schema-example.sh --docs 10000   # CI fast path (~2–5 min)
#   ./scripts/run-minio-large-schema-example.sh --tier l1 --skip-ingest  # bench only
#   ./scripts/run-minio-large-schema-example.sh --skip-warm              # cold + filter/hybrid only
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export LARGE_PREFLIGHT_ROOT="$ROOT"
# shellcheck source=scripts/lib/large-benchmark-preflight.sh
source "$ROOT/scripts/lib/large-benchmark-preflight.sh"

TIER="${OPENPUFFER_BENCH_TIER:-l1}"
DOCS="${OPENPUFFER_BENCH_DOCS:-100000}"
SKIP_INGEST=0
WARM_MODE=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier=*) TIER="${1#*=}" ;;
    --tier) shift; TIER="${1:?}" ;;
    --docs=*) DOCS="${1#*=}" ;;
    --docs) shift; DOCS="${1:?--docs requires a number}" ;;
    --skip-ingest) SKIP_INGEST=1 ;;
    --warm) WARM_MODE=1 ;;
    --skip-warm) WARM_MODE=0 ;;
    -h|--help)
      sed -n '2,13p' "$0"
      exit 0
      ;;
  esac
  shift
done
[[ "${OPENPUFFER_BENCH_WARM:-}" == "1" ]] && WARM_MODE=1
[[ "${OPENPUFFER_BENCH_WARM:-}" == "0" ]] && WARM_MODE=0

[[ "$TIER" == "l1" ]] || {
  echo "schema example script only supports --tier l1" >&2
  exit 1
}
[[ "$DOCS" == "100000" || "$DOCS" == "10000" ]] || {
  echo "schema example supports --docs 10000 (CI fast path) or 100000 (committed L1 example)" >&2
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
# 10k fast path writes *-schema-minio-10k.example.json (committed CI exemplar; same keys as 100k example).
if [[ "$DOCS" == "10000" ]]; then
  SCHEMA_SUFFIX="-10k"
else
  SCHEMA_SUFFIX=""
fi
export OPENPUFFER_INGEST_DOCS="$DOCS"
export OPENPUFFER_BENCH_DOCS="$DOCS"
export OPENPUFFER_BENCH_RESULTS="$ROOT/benchmarks/results/large-aws-l1-schema-minio${SCHEMA_SUFFIX}.example.json"
export OPENPUFFER_INGEST_ENVIRONMENT=minio
export OPENPUFFER_INGEST_RESULTS="$ROOT/benchmarks/results/ingest-large-l1-schema-minio${SCHEMA_SUFFIX}.example.json"
export OPENPUFFER_BENCH_INGEST_JSON="$OPENPUFFER_INGEST_RESULTS"

large_preflight_toolchain
large_preflight_ann_version
large_preflight_validate_tier_workload "$TIER" "$ROOT"

if ! curl -sf "${ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
  echo "MinIO not reachable at ${ENDPOINT}; start: ./scripts/run-integration-s3.sh" >&2
  exit 1
fi

echo "run-minio-large-schema-example: tier=${TIER} docs=${DOCS} endpoint=${ENDPOINT} bucket=${BUCKET} warm=${WARM_MODE}"
echo "  NOT for COMPARISON.md / tpuf tables — environment=minio schema validation only"

export OPENPUFFER_INGEST_DELETE_FIRST="${OPENPUFFER_INGEST_DELETE_FIRST:-1}"
# Full schema example must run serve + upsert (unset dev-shell shortcuts).
unset OPENPUFFER_INGEST_SKIP_UPSERT OPENPUFFER_INGEST_SKIP_SERVE OPENPUFFER_BENCH_SKIP_SERVE || true

if [[ "$SKIP_INGEST" != "1" ]]; then
  ./scripts/ingest-large.sh --tier "$TIER"
fi

BENCH_ARGS=(--tier "$TIER")
[[ "$WARM_MODE" == "1" ]] && BENCH_ARGS+=(--warm)
./scripts/bench-large.sh "${BENCH_ARGS[@]}"

export SCHEMA_MINIO_BENCH_RESULTS="${OPENPUFFER_BENCH_RESULTS}"
export SCHEMA_MINIO_INGEST_RESULTS="${OPENPUFFER_INGEST_RESULTS}"
export SCHEMA_MINIO_WARM_MODE="$WARM_MODE"
python3 - <<'PY'
import json
import os
from pathlib import Path

bench_path = Path(os.environ["SCHEMA_MINIO_BENCH_RESULTS"])
ingest_path = Path(os.environ["SCHEMA_MINIO_INGEST_RESULTS"])
warm_mode = os.environ.get("SCHEMA_MINIO_WARM_MODE", "1") == "1"

d = json.loads(bench_path.read_text())
assert d["environment"] == "minio", d.get("environment")
assert d["benchmark"] == "cold_large_l1"
for k in (
    "tier",
    "workload_dir",
    "recall_at_10",
    "p50_query_latency_ms",
    "storage_roundtrips",
    "preferred_ann_version",
    "index_cursor_eq_wal_commit_seq",
    "cold_query_runs",
    "filter_query_runs",
    "hybrid_query_runs",
    "ingest_summary_path",
    "ingest_elapsed_secs",
    "index_wait_sec",
    "ingest_timing",
):
    assert k in d, k
assert len(d["filter_query_runs"]) >= 1, "filter_query_runs"
assert len(d["hybrid_query_runs"]) >= 1, "hybrid_query_runs"
timing = d["ingest_timing"]
for tk in (
    "upsert_wall_sec",
    "index_wait_sec",
    "total_wall_sec",
    "batch_count",
    "batch_latency_ms",
):
    assert tk in timing, tk

if warm_mode:
    for k in (
        "p50_warm_query_latency_ms",
        "p95_warm_query_latency_ms",
        "warm_query_runs",
        "warm_consistency",
        "warm_runs",
        "warm_filter_query_runs",
        "warm_hybrid_query_runs",
    ):
        assert k in d, k
    assert d["warm_query_runs"] is not None and d["warm_query_runs"] > 0
    assert len(d.get("warm_filter_query_runs") or []) >= 1
    assert len(d.get("warm_hybrid_query_runs") or []) >= 1

i = json.loads(ingest_path.read_text())
assert i["environment"] == "minio"
assert i["benchmark"] == "ingest_large"
assert "ingest_timing" in i and "batch_runs" in i["ingest_timing"]
assert len(i["ingest_timing"]["batch_runs"]) >= 1

print("schema validation OK:", bench_path)
PY

echo "Wrote schema example: ${OPENPUFFER_BENCH_RESULTS}"