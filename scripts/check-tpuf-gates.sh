#!/usr/bin/env bash
# Post-hoc turbopuffer large-tier SLO gate on tpuf-*.json.
# Enforces the same gates as run_benchmark.py when TURBOPUFFER_BENCH_ENFORCE_GATES=1:
#   environment turbopuffer:{region}, tpuf_region, cold_query_runs==7,
#   index_up_to_date, recall@10>=tier gate.
#
# Usage:
#   ./scripts/check-tpuf-gates.sh benchmarks/results/tpuf-l1.json
#   ./scripts/check-tpuf-gates.sh --tier l2 path/to/tpuf-l2.json
#   TURBOPUFFER_BENCH_ENFORCE_GATES=0 ./scripts/check-tpuf-gates.sh …   # always exit 0
#
# Wired from ./scripts/run-tpuf-large-benchmark.sh after run_benchmark.py.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck source=scripts/lib/large-benchmark-tpuf-gates.sh
source "$ROOT/scripts/lib/large-benchmark-tpuf-gates.sh"

TIER=""
JSON_PATH=""
ENFORCE_GATES="${TURBOPUFFER_BENCH_ENFORCE_GATES:-1}"

usage() {
  sed -n '2,14p' "$0"
  exit "${LARGE_BENCHMARK_EXIT_USAGE:-64}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier=*) TIER="${1#*=}" ;;
    --tier) shift; TIER="${1:?--tier requires l1|l2|l3}" ;;
    -h|--help) usage ;;
    -*)
      echo "check-tpuf-gates: unknown option: $1" >&2
      usage
      ;;
    *)
      JSON_PATH="$1"
      ;;
  esac
  shift
done

if [[ -z "$JSON_PATH" ]]; then
  echo "check-tpuf-gates: JSON path required" >&2
  usage
fi

if [[ ! -f "$JSON_PATH" ]]; then
  echo "check-tpuf-gates: file not found: ${JSON_PATH}" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "check-tpuf-gates: jq required" >&2
  exit 1
fi

ENV="$(jq -r '.environment // empty' "$JSON_PATH")"
JSON_TIER="$(jq -r '.tier // empty' "$JSON_PATH")"
BENCHMARK="$(jq -r '.benchmark // empty' "$JSON_PATH")"
[[ -z "$TIER" && -n "$JSON_TIER" ]] && TIER="$JSON_TIER"
[[ -z "$TIER" ]] && TIER="l1"

case "$TIER" in
  l1|l2|l3) ;;
  *)
    echo "check-tpuf-gates: unknown tier: ${TIER}" >&2
    exit 1
    ;;
esac

expected_benchmark="cold_tpuf_${TIER}"
if [[ -n "$BENCHMARK" && "$BENCHMARK" != "$expected_benchmark" ]]; then
  echo "check-tpuf-gates: benchmark ${BENCHMARK} != ${expected_benchmark}" >&2
  exit 1
fi

RECALL_GATE="$(large_benchmark_tpuf_tier_recall_gate "$TIER")"

if [[ "$ENFORCE_GATES" == "0" ]]; then
  echo "check-tpuf-gates: skipped (TURBOPUFFER_BENCH_ENFORCE_GATES=0) ${JSON_PATH}"
  exit 0
fi

if [[ "$ENV" != turbopuffer:* ]]; then
  echo "check-tpuf-gates: skipped (environment=${ENV:-<unset>}, tpuf SLO gates apply only to turbopuffer:*)"
  exit 0
fi

echo "check-tpuf-gates: ${JSON_PATH} tier=${TIER} (cold_query_runs=${LARGE_BENCHMARK_TPUF_COLD_QUERY_RUNS}, recall@10>=${RECALL_GATE}, index_up_to_date)"

failures=()
while IFS= read -r line; do
  [[ -n "$line" ]] || continue
  failures+=("$line")
done < <(large_benchmark_tpuf_gate_failures "$JSON_PATH" "$RECALL_GATE" || true)

if [[ "${#failures[@]}" -gt 0 ]]; then
  echo "check-tpuf-gates: FAILED (${#failures[@]} gate(s)):" >&2
  for f in "${failures[@]}"; do
    echo "  - ${f}" >&2
  done
  echo "Set TURBOPUFFER_BENCH_ENFORCE_GATES=0 to record without failing." >&2
  exit 1
fi

echo "check-tpuf-gates: OK (all tpuf large-tier gates passed)"
exit 0