#!/usr/bin/env bash
# Phase 3.3 — preflight before live id-overlap spot-check (both namespaces indexed).
#
# Usage:
#   ./scripts/preflight-id-overlap.sh --tier l1
#   ./scripts/preflight-id-overlap.sh --tier l2 --skip-key   # openpuffer-only meta check
#
# Environment:
#   OPENPUFFER_BASE_URL (default http://127.0.0.1:8080)
#   OPENPUFFER_BENCH_NAMESPACE / OPENPUFFER_NAMESPACE (default bench-large-{tier})
#   TURBOPUFFER_API_KEY (required unless --skip-key)
#   TURBOPUFFER_BENCH_NAMESPACE (default bench-tpuf-{date}-{tier})
#   TURBOPUFFER_REGION (default aws-us-east-1)
#
# See benchmarks/cross_check/README.md and docs/BENCHMARKS.md Phase 3.3.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export LARGE_PREFLIGHT_ROOT="$ROOT"
# shellcheck source=scripts/lib/large-benchmark-preflight.sh
source "$ROOT/scripts/lib/large-benchmark-preflight.sh"

TIER="${OPENPUFFER_ID_OVERLAP_TIER:-l1}"
SKIP_KEY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier=*) TIER="${1#*=}"; shift ;;
    --tier)
      shift
      TIER="${1:?--tier requires l1|l2|l3}"
      shift
      ;;
    --skip-key) SKIP_KEY=1; shift ;;
    -h|--help)
      sed -n '2,20p' "$0"
      exit 0
      ;;
    *) large_benchmark_exit_usage "preflight-id-overlap: unknown argument: $1" ;;
  esac
done

case "$TIER" in
  l1|l2|l3) ;;
  *)
    large_benchmark_exit_preflight "preflight-id-overlap: unknown tier ${TIER} (use l1, l2, or l3)"
    ;;
esac

large_preflight_toolchain
large_preflight_ann_version
large_preflight_validate_tier_workload "$TIER" "$ROOT"

OP_BASE="${OPENPUFFER_BASE_URL:-http://127.0.0.1:8080}"
OP_NS="${OPENPUFFER_BENCH_NAMESPACE:-${OPENPUFFER_NAMESPACE:-bench-large-${TIER}}}"

echo "preflight-id-overlap: tier=${TIER}"
echo "preflight-id-overlap: OPENPUFFER_BASE_URL=${OP_BASE}"
echo "preflight-id-overlap: OPENPUFFER_BENCH_NAMESPACE=${OP_NS}"

if [[ "$SKIP_KEY" != "1" ]]; then
  if [[ -z "${TURBOPUFFER_API_KEY:-}" ]]; then
    echo "preflight-id-overlap: TURBOPUFFER_API_KEY unset (run G4 ingest first; or --skip-key for openpuffer-only)" >&2
    large_benchmark_exit_preflight
  fi
  echo "preflight-id-overlap: TURBOPUFFER_API_KEY=set"
  echo "preflight-id-overlap: TURBOPUFFER_BENCH_NAMESPACE=${TURBOPUFFER_BENCH_NAMESPACE:-bench-tpuf-$(date +%F)-${TIER}}"
  echo "preflight-id-overlap: TURBOPUFFER_REGION=${TURBOPUFFER_REGION:-aws-us-east-1}"
  large_preflight_tpuf_python_deps "$ROOT"
else
  echo "preflight-id-overlap: --skip-key (skipping turbopuffer namespace check)"
  export TURBOPUFFER_API_KEY="${TURBOPUFFER_API_KEY:-}"
fi

if [[ "$SKIP_KEY" == "1" ]]; then
  meta="$(curl -sf "${OP_BASE%/}/v1/namespaces/${OP_NS}" 2>/dev/null || true)"
  if [[ -z "$meta" ]]; then
    echo "preflight-id-overlap: openpuffer namespace ${OP_NS} not found at ${OP_BASE}" >&2
    large_benchmark_exit_preflight
  fi
  commit="$(echo "$meta" | jq -r '.wal_commit_seq // 0')"
  if [[ "$commit" == "0" ]]; then
    echo "preflight-id-overlap: openpuffer namespace ${OP_NS} empty (wal_commit_seq=0)" >&2
    echo "  run: ./scripts/run-aws-large-benchmark.sh --tier ${TIER}" >&2
    large_benchmark_exit_preflight
  fi
  echo "preflight-id-overlap: openpuffer OK (wal_commit_seq=${commit})"
  exit 0
fi

python3 benchmarks/cross_check/run_spotcheck.py --tier "$TIER" --preflight-only
echo "preflight-id-overlap: OK (tier=${TIER})"