#!/usr/bin/env bash
# Phase 3.3 — cross-engine id overlap spot-check (see benchmarks/cross_check/README.md).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TIER="${OPENPUFFER_ID_OVERLAP_TIER:-l1}"
EXTRA=()
LIVE=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier=*) TIER="${1#*=}" ;;
    --tier)
      shift
      TIER="${1:?--tier requires l1|l2|l3}"
      ;;
    --dry-run|-n|--mock|--preflight-only) EXTRA+=("$1"); LIVE=0 ;;
    -h|--help)
      sed -n '2,14p' "$0"
      exit 0
      ;;
    *) EXTRA+=("$1") ;;
  esac
  shift
done

if [[ "$LIVE" == "1" ]]; then
  if [[ -x "$ROOT/scripts/preflight-id-overlap.sh" ]]; then
    "$ROOT/scripts/preflight-id-overlap.sh" --tier "$TIER"
  fi
fi

exec python3 benchmarks/cross_check/run_spotcheck.py --tier "$TIER" "${EXTRA[@]}"