#!/usr/bin/env bash
# G4 operator wrapper: MinIO G2 preflight → turbopuffer API key/region → run_benchmark.py.
# Produces benchmarks/results/tpuf-{tier}.json for Phase 7 / COMPARISON.md.
#
# Usage:
#   ./scripts/run-tpuf-large-benchmark.sh --tier l1
#   ./scripts/run-tpuf-large-benchmark.sh --tier l3
#   ./scripts/run-tpuf-large-benchmark.sh --preflight-only   # G2 + tpuf env, no API spend
#   ./scripts/run-tpuf-large-benchmark.sh --dry-run          # print plan + env checklist
#   ./scripts/run-tpuf-large-benchmark.sh --skip-g2          # skip MinIO correctness subset
#   ./scripts/run-tpuf-large-benchmark.sh --skip-ingest      # query existing namespace
#   ./scripts/run-tpuf-large-benchmark.sh --warm             # cold + filter/hybrid + warm phase
#
# Environment: see large_preflight_print_tpuf_operator_env in scripts/lib/large-benchmark-preflight.sh
# and docs/BENCHMARKS.md § large-dataset program operator runbook.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export LARGE_PREFLIGHT_ROOT="$ROOT"
# shellcheck source=scripts/lib/large-benchmark-preflight.sh
source "$ROOT/scripts/lib/large-benchmark-preflight.sh"
# shellcheck source=scripts/lib/estimate-large-benchmark-cost.sh
source "$ROOT/scripts/lib/estimate-large-benchmark-cost.sh"
# shellcheck source=scripts/lib/tier-validate.sh
source "$ROOT/scripts/lib/tier-validate.sh"

TIER="${TURBOPUFFER_BENCH_TIER:-l1}"
DRY_RUN=0
PREFLIGHT_ONLY=0
SKIP_G2=0
SKIP_INGEST=0
WARM_MODE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run|-n) DRY_RUN=1; shift ;;
    --preflight-only) PREFLIGHT_ONLY=1; shift ;;
    --skip-g2) SKIP_G2=1; shift ;;
    --skip-ingest) SKIP_INGEST=1; shift ;;
    --warm) WARM_MODE=1; shift ;;
    --tier=*) TIER="${1#*=}"; shift ;;
    --tier)
      shift
      TIER="${1:?--tier requires l1|l2|l3}"
      shift
      ;;
    -h|--help)
      sed -n '2,20p' "$0"
      large_preflight_print_tpuf_operator_env
      exit 0
      ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

validate_tier "$TIER"

RESULTS_DEFAULT="$ROOT/benchmarks/results/tpuf-${TIER}.json"
RESULTS="${TURBOPUFFER_BENCH_RESULTS:-$RESULTS_DEFAULT}"
REGION="${TURBOPUFFER_REGION:-aws-us-east-1}"

run_plan_dry() {
  echo "run-tpuf-large-benchmark dry-run OK"
  echo "  tier=${TIER}"
  echo "  region=${REGION}"
  echo "  results=${RESULTS}"
  echo "  warm_mode=${WARM_MODE}"
  echo "  steps: $([[ "$SKIP_G2" == 1 ]] && echo 'skip-g2' || echo 'g2-subset') → tpuf-preflight → run_benchmark.py"
  large_benchmark_cost_print "$TIER" "$WARM_MODE" tpuf
  large_preflight_print_tpuf_operator_env
  if [[ -n "${TURBOPUFFER_API_KEY:-}" ]]; then
    echo "  TURBOPUFFER_API_KEY=set"
  else
    echo "  TURBOPUFFER_API_KEY unset (required for live run)"
  fi
  export TURBOPUFFER_BENCH_DRY_RUN=1
  python3 benchmarks/tpuf_driver/run_benchmark.py --tier "$TIER" --dry-run
  exit 0
}

[[ "$DRY_RUN" == "1" ]] && run_plan_dry

large_preflight_toolchain
large_preflight_ann_version
large_preflight_validate_tier_workload "$TIER" "$ROOT"

if [[ "$SKIP_G2" != "1" ]]; then
  large_preflight_run_g2_subset "$ROOT"
fi

if [[ -x "$ROOT/scripts/preflight-tpuf.sh" ]]; then
  tpuf_preflight_args=(--tier "$TIER")
  [[ "$WARM_MODE" == "1" ]] && tpuf_preflight_args+=(--warm)
  "$ROOT/scripts/preflight-tpuf.sh" "${tpuf_preflight_args[@]}" || \
    large_benchmark_exit_preflight "preflight-tpuf failed (API key/region/RTT); fix before live G4"
else
  large_preflight_validate_tpuf_env
  large_preflight_tpuf_python_deps "$ROOT"
fi

echo "run-tpuf-large-benchmark: tier=${TIER} region=${REGION} results=${RESULTS}"
if [[ -n "${TURBOPUFFER_BENCH_NAMESPACE:-}" ]]; then
  echo "  namespace=${TURBOPUFFER_BENCH_NAMESPACE}"
fi

if [[ "$PREFLIGHT_ONLY" == "1" ]]; then
  echo "preflight-only: OK (G2 subset + tpuf env + workload). Run without --preflight-only for live benchmark."
  exit 0
fi

export TURBOPUFFER_BENCH_TIER="$TIER"
export TURBOPUFFER_BENCH_RESULTS="$RESULTS"
export TURBOPUFFER_REGION="$REGION"
export TURBOPUFFER_BENCH_ENFORCE_GATES="${TURBOPUFFER_BENCH_ENFORCE_GATES:-1}"
export TURBOPUFFER_BENCH_DELETE_FIRST="${TURBOPUFFER_BENCH_DELETE_FIRST:-1}"

TPUF_ARGS=(--tier "$TIER")
if [[ "$SKIP_INGEST" == "1" ]]; then
  TPUF_ARGS+=(--skip-ingest)
fi
if [[ "$WARM_MODE" == "1" ]]; then
  TPUF_ARGS+=(--warm)
  export TURBOPUFFER_BENCH_WARM=1
fi

echo "==> tpuf run_benchmark.py ${TPUF_ARGS[*]}"
python3 benchmarks/tpuf_driver/run_benchmark.py "${TPUF_ARGS[@]}"

if [[ -f "$RESULTS" ]]; then
  echo "==> check-tpuf-gates ${RESULTS}"
  export TURBOPUFFER_BENCH_ENFORCE_GATES="${TURBOPUFFER_BENCH_ENFORCE_GATES:-1}"
  ./scripts/check-tpuf-gates.sh --tier "$TIER" "$RESULTS"
fi

echo "G4 complete: ${RESULTS}"
echo "Next: ./scripts/run-id-overlap-spotcheck.sh --tier ${TIER}  # Phase 3.3 (after G3 AWS JSON)"
echo "      ./scripts/render-report.sh --date $(date +%F)"