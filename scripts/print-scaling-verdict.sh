#!/usr/bin/env bash
# Print a single-paragraph operator verdict for openpuffer vs turbopuffer scaling.
# Uses committed benchmarks/results/op-scaling-*.json (offline; no MinIO bench).
set -euo pipefail

usage() {
  cat <<'EOF'
print-scaling-verdict.sh — one-paragraph operator verdict (openpuffer vs tpuf scaling).

Purpose:
  Summarize scaling shape vs turbopuffer 10M×1024 official reference from committed
  op-scaling JSON (canonical linear extrap, warm ratios, ingest docs/s when present).

Usage:
  ./scripts/print-scaling-verdict.sh
  ./scripts/print-scaling-verdict.sh -h|--help

Environment:
  (none required; offline)

Input tiers (committed benchmarks/results/):
  op-scaling-{10k,50k,100k}.json (+ warm / synthetic128 if present)
  tpuf-official-reference.json

Output:
  Single paragraph on stdout (no files written).

Quickstart:
  benchmarks/SCALING_VS_TPUF_QUICKSTART.md  (step 4 after make bench-compare-tpuf)
EOF
}

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

exec python3 "$ROOT/benchmarks/report/compare_op_scaling_to_tpuf.py" --verdict-only