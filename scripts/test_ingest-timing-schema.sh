#!/usr/bin/env bash
# Offline schema checks for ingest-large JSON timing fields (§4.1 ingest breakdown).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

python3 - <<'PY'
import json
from pathlib import Path

REQUIRED_TOP = (
    "ingest_elapsed_secs",
    "index_wait_sec",
    "ingest_total_wall_sec",
    "ingest_docs_per_sec",
    "ingest_batches_per_sec",
    "ingest_timing",
)
TIMING_REQUIRED = (
    "upsert_wall_sec",
    "index_wait_sec",
    "total_wall_sec",
    "batch_count",
    "batches_per_sec",
    "docs_per_sec",
    "batch_latency_ms",
    "batch_runs",
)
LAT_REQUIRED = ("p50", "p95", "min", "max")

paths = [
    Path("benchmarks/results/ingest-large-l1-schema-minio.example.json"),
    Path("benchmarks/report/fixtures/ingest-large-l1.json"),
]

for path in paths:
    data = json.loads(path.read_text())
    assert data["benchmark"] == "ingest_large", path
    for key in REQUIRED_TOP:
        assert key in data, (path, key)
    timing = data["ingest_timing"]
    for key in TIMING_REQUIRED:
        assert key in timing, (path, key)
    for key in LAT_REQUIRED:
        assert key in timing["batch_latency_ms"], (path, key)
    assert data["ingest_elapsed_secs"] == timing["upsert_wall_sec"]
    assert data["index_wait_sec"] == timing["index_wait_sec"]
    assert data["ingest_total_wall_sec"] == timing["total_wall_sec"]
    assert isinstance(timing["batch_runs"], list)
    if timing["batch_runs"]:
        run0 = timing["batch_runs"][0]
        assert {"batch", "file", "latency_ms"} <= set(run0.keys())

bench = json.loads(
    Path("benchmarks/report/fixtures/large-aws-l1.json").read_text()
)
for key in ("ingest_elapsed_secs", "index_wait_sec", "ingest_timing", "ingest_summary_path"):
    assert key in bench, key

print("ingest-timing schema tests OK")
PY

grep -q 'ingest_timing' scripts/ingest-large.sh
grep -q 'load_ingest_summary_for_bench' scripts/bench-large.sh
grep -q 'Index wait (s)' scripts/render-report.sh
./scripts/ingest-large.sh --dry-run >/dev/null
./scripts/bench-large.sh --dry-run >/dev/null