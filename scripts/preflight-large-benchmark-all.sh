#!/usr/bin/env bash
# Run G3 + G4 + Phase 3.3 preflights in one pass (offline cost/env or live EC2 spend gates).
#
# Default (offline, no API keys / no S3 head-bucket):
#   ./scripts/preflight-large-benchmark-all.sh
#   ./scripts/preflight-large-benchmark-all.sh --tier l1
#
# Live (EC2 + creds; same checks as run-*-large-benchmark --preflight-only):
#   ./scripts/preflight-large-benchmark-all.sh --live --tier l1
#
# Skip id-overlap when openpuffer is not up yet (aws+tpuf checks only):
#   ./scripts/preflight-large-benchmark-all.sh --skip-overlap
#
# See benchmarks/OPERATOR_RUNBOOK_QUICK.md and docs/BENCHMARKS.md § G3/G4.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export LARGE_PREFLIGHT_ROOT="$ROOT"
# shellcheck source=scripts/lib/large-benchmark-preflight.sh
source "$ROOT/scripts/lib/large-benchmark-preflight.sh"
# shellcheck source=scripts/lib/tier-validate.sh
source "$ROOT/scripts/lib/tier-validate.sh"

TIER="${OPENPUFFER_BENCH_TIER:-l1}"
LIVE=0
SKIP_OVERLAP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier=*) TIER="${1#*=}"; shift ;;
    --tier)
      shift
      TIER="${1:?--tier requires l1|l2|l3}"
      shift
      ;;
    --live) LIVE=1; shift ;;
    --skip-overlap) SKIP_OVERLAP=1; shift ;;
    -h|--help)
      sed -n '2,16p' "$0"
      exit 0
      ;;
    *) large_benchmark_exit_usage "preflight-large-benchmark-all: unknown argument: $1" ;;
  esac
done

validate_tier "$TIER" "preflight-large-benchmark-all"

run_step() {
  local label="$1"
  shift
  echo ""
  echo "==> ${label}"
  "$@"
}

if [[ "$LIVE" == "1" ]]; then
  echo "preflight-large-benchmark-all: live mode (tier=${TIER})"
  run_step "G3 preflight-aws-ec2" "$ROOT/scripts/preflight-aws-ec2.sh" --tier "$TIER"
  run_step "G4 preflight-tpuf" "$ROOT/scripts/preflight-tpuf.sh" --tier "$TIER"
  if [[ "$SKIP_OVERLAP" != "1" ]]; then
    run_step "Phase 3.3 preflight-id-overlap" "$ROOT/scripts/preflight-id-overlap.sh" --tier "$TIER"
  else
    echo "==> Phase 3.3 preflight-id-overlap (skipped --skip-overlap)"
  fi
else
  echo "preflight-large-benchmark-all: offline mode (tier=${TIER}; aws --dry-run, tpuf/id-overlap --skip-key)"
  run_step "G3 preflight-aws-ec2 (dry-run)" "$ROOT/scripts/preflight-aws-ec2.sh" --dry-run --tier "$TIER"
  run_step "G4 preflight-tpuf (skip-key, skip-rtt)" \
    "$ROOT/scripts/preflight-tpuf.sh" --tier "$TIER" --skip-key --skip-rtt
  if [[ "$SKIP_OVERLAP" != "1" ]]; then
    run_step "Phase 3.3 preflight-id-overlap (skip-key)" \
      "$ROOT/scripts/preflight-id-overlap.sh" --tier "$TIER" --skip-key
  else
    echo "==> Phase 3.3 preflight-id-overlap (skipped --skip-overlap)"
  fi
fi

echo ""
echo "preflight-large-benchmark-all: OK (tier=${TIER} live=${LIVE} skip_overlap=${SKIP_OVERLAP})"