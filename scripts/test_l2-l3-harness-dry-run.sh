#!/usr/bin/env bash
# Offline L2/L3 harness validation (ingest, bench, G3, G4, program, id-overlap, render-report).
# No AWS/tpuf credentials required.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

for tier in l2 l3; do
  echo "==> tier=${tier} ingest-large --dry-run"
  ./scripts/ingest-large.sh --tier "$tier" --dry-run >/dev/null
  echo "==> tier=${tier} bench-large --dry-run"
  ./scripts/bench-large.sh --tier "$tier" --dry-run >/dev/null
  echo "==> tier=${tier} run-aws-large-benchmark --dry-run"
  ./scripts/run-aws-large-benchmark.sh --tier "$tier" --dry-run >/dev/null
  echo "==> tier=${tier} run-tpuf-large-benchmark --dry-run"
  ./scripts/run-tpuf-large-benchmark.sh --tier "$tier" --dry-run --skip-g2 >/dev/null
  echo "==> tier=${tier} tpuf driver --dry-run"
  python3 benchmarks/tpuf_driver/run_benchmark.py --tier "$tier" --dry-run >/dev/null
  echo "==> tier=${tier} id-overlap --dry-run"
  ./scripts/run-id-overlap-spotcheck.sh --tier "$tier" --dry-run >/dev/null
  echo "==> tier=${tier} run-large-benchmark-program --dry-run"
  OPENPUFFER_REPORT_OUTPUT="/tmp/openpuffer-program-${tier}-dry-run.md" \
    OPENPUFFER_REPORT_DATE=2099-06-04 \
    ./scripts/run-large-benchmark-program.sh --tier "$tier" --dry-run --skip-g2 >/dev/null
  echo "==> tier=${tier} render-report --dry-run"
  OPENPUFFER_REPORT_DATE=2099-06-04 ./scripts/render-report.sh --dry-run --tier "$tier" \
    --output "/tmp/openpuffer-render-report-${tier}-dry-run.md" >/dev/null
done

echo "test_l2-l3-harness-dry-run: OK"