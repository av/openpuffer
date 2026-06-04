#!/usr/bin/env bash
# Offline schema checks for bench-large filter/hybrid query runs (mirror tpuf JSON).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

python3 - <<'PY'
import json
from pathlib import Path

RUN_KEYS = ("query_name", "query_kind", "latency_ms")

def check_runs(path: Path, runs: list, label: str) -> None:
    assert isinstance(runs, list), (path, label)
    for entry in runs:
        for key in RUN_KEYS:
            assert key in entry, (path, label, entry)
        assert entry["query_kind"] in ("filter", "hybrid"), (path, entry)

for path_str in (
    "benchmarks/report/fixtures/large-aws-l1.json",
    "benchmarks/report/fixtures/tpuf-l1.json",
    "benchmarks/results/large-aws-l1-schema-minio.example.json",
):
    path = Path(path_str)
    data = json.loads(path.read_text())
    check_runs(path, data.get("filter_query_runs") or [], "filter_query_runs")
    check_runs(path, data.get("hybrid_query_runs") or [], "hybrid_query_runs")
    if path_str.endswith("large-aws-l1.json") or "schema-minio.example.json" in path_str:
        warm_f = data.get("warm_filter_query_runs") or []
        warm_h = data.get("warm_hybrid_query_runs") or []
        if warm_f:
            check_runs(path, warm_f, "warm_filter_query_runs")
        if warm_h:
            check_runs(path, warm_h, "warm_hybrid_query_runs")
        if "schema-minio.example.json" in path_str:
            for key in (
                "p50_warm_query_latency_ms",
                "warm_query_runs",
                "ingest_timing",
                "ingest_elapsed_secs",
            ):
                assert key in data, (path, key)

print("bench-large secondary schema tests OK")
PY

grep -q 'filter_query_runs' scripts/bench-large.sh
grep -q 'run_filter_hybrid_queries' scripts/bench-large.sh
grep -q 'filter_query_runs' scripts/render-report.sh
./scripts/bench-large.sh --dry-run >/dev/null
./scripts/bench-large.sh --warm --dry-run >/dev/null