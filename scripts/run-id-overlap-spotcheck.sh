#!/usr/bin/env bash
# Phase 3.3 — cross-engine id overlap spot-check (see benchmarks/cross_check/README.md).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TIER="${OPENPUFFER_ID_OVERLAP_TIER:-l1}"
EXTRA=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier=*) TIER="${1#*=}" ;;
    --tier)
      shift
      TIER="${1:?--tier requires l1|l2|l3}"
      ;;
    --dry-run|-n|--mock) EXTRA+=("$1") ;;
    -h|--help)
      sed -n '2,12p' "$0"
      exit 0
      ;;
    *) EXTRA+=("$1") ;;
  esac
  shift
done

exec python3 benchmarks/cross_check/run_spotcheck.py --tier "$TIER" "${EXTRA[@]}"