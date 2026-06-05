#!/usr/bin/env bash
# Print openpuffer power-law extrapolation vs turbopuffer 10M official reference.
set -euo pipefail

usage() {
  cat <<'EOF'
compare-op-scaling-to-tpuf.sh — extrapolate openpuffer MinIO tiers vs tpuf 10M reference.

Purpose:
  Read committed op-scaling-*.json + tpuf-official-reference.json; fit doc-count models,
  print tables, EXTRAP_JSON line, and markdown appendix; write dashboard summary JSON.

Usage:
  ./scripts/compare-op-scaling-to-tpuf.sh
  ./scripts/compare-op-scaling-to-tpuf.sh --write-summary
  ./scripts/compare-op-scaling-to-tpuf.sh --csv
  ./scripts/compare-op-scaling-to-tpuf.sh --model=linear|power_law|log_linear
  ./scripts/compare-op-scaling-to-tpuf.sh --dry-run
  ./scripts/compare-op-scaling-to-tpuf.sh -h|--help
  make bench-compare-tpuf

Environment:
  (none required; offline on committed JSON)
  TURBOPUFFER_API_KEY  optional — live 10k tpuf point not used by default

Input tiers (committed benchmarks/results/):
  op-scaling-10k.json, op-scaling-50k.json, op-scaling-100k.json
  op-scaling-10k-warm.json, op-scaling-100k-warm.json (optional warm ratios)
  op-scaling-10k-synthetic128.json (optional workload gate)
  tpuf-official-reference.json (10M × 1024 cold/warm official)

Output files:
  benchmarks/results/scaling-comparison-summary.json  (every full run)
  benchmarks/results/scaling-comparison.csv             (--csv)
  stdout: tables, EXTRAP_JSON=..., appendix snippet

Quickstart:
  benchmarks/SCALING_VS_TPUF_QUICKSTART.md
EOF
}

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

ARGS=()
for arg in "$@"; do
  case "$arg" in
    -h|--help)
      usage
      exit 0
      ;;
    *) ARGS+=("$arg") ;;
  esac
done

exec python3 "$ROOT/benchmarks/report/compare_op_scaling_to_tpuf.py" "${ARGS[@]}"