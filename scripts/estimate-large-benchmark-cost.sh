#!/usr/bin/env bash
# Print order-of-magnitude AWS EC2 hours, S3 PUT/GET, and turbopuffer API volume for L1/L2/L3.
# Based on docs/BENCHMARKS.md and PLAN_LARGE_DATASET_BENCHMARK.md assumptions (not a live quote).
#
# Usage:
#   ./scripts/estimate-large-benchmark-cost.sh --tier l1
#   ./scripts/estimate-large-benchmark-cost.sh --tier l3 --warm
#   ./scripts/estimate-large-benchmark-cost.sh --tier l2 --scope aws
#   ./scripts/estimate-large-benchmark-cost.sh --tier l1 --json
#
# Environment: none required (offline).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/lib/estimate-large-benchmark-cost.sh
source "$ROOT/scripts/lib/estimate-large-benchmark-cost.sh"

TIER="${OPENPUFFER_BENCH_TIER:-l1}"
WARM=0
SCOPE="all"
JSON=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier=*) TIER="${1#*=}"; shift ;;
    --tier)
      shift
      TIER="${1:?--tier requires l1|l2|l3}"
      shift
      ;;
    --warm) WARM=1; shift ;;
    --scope=*) SCOPE="${1#*=}"; shift ;;
    --scope)
      shift
      SCOPE="${1:?--scope requires aws|tpuf|all}"
      shift
      ;;
    --json) JSON=1; shift ;;
    -h|--help)
      sed -n '2,14p' "$0"
      exit 0
      ;;
    *) echo "estimate-large-benchmark-cost: unknown argument: $1" >&2; exit 1 ;;
  esac
done

case "$TIER" in
  l1|l2|l3) ;;
  *)
    echo "estimate-large-benchmark-cost: unknown tier ${TIER} (use l1, l2, or l3)" >&2
    exit 1
    ;;
esac

if [[ "$JSON" == "1" ]]; then
  large_benchmark_cost_compute "$TIER" "$WARM"
  exit 0
fi

large_benchmark_cost_print "$TIER" "$WARM" "$SCOPE"