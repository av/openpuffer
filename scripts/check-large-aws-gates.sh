#!/usr/bin/env bash
# Post-hoc AWS large-tier SLO gate on large-aws-*.json.
# Enforces the same gates as bench-large.sh when OPENPUFFER_BENCH_ENFORCE_GATES=1 and environment=aws-s3:
#   preferred_ann_version==3, index caught up, storage_roundtrips<=4, recall@10>=tier gate, p50<600ms.
#
# Usage:
#   ./scripts/check-large-aws-gates.sh benchmarks/results/large-aws-l1.json
#   ./scripts/check-large-aws-gates.sh --tier l2 path/to/large-aws-l2.json
#   OPENPUFFER_BENCH_ENFORCE_GATES=0 ./scripts/check-large-aws-gates.sh …   # always exit 0
#
# Wired from ./scripts/run-aws-large-benchmark.sh after bench-large.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck source=scripts/lib/large-benchmark-aws-gates.sh
source "$ROOT/scripts/lib/large-benchmark-aws-gates.sh"

TIER=""
JSON_PATH=""
ENFORCE_GATES="${OPENPUFFER_BENCH_ENFORCE_GATES:-1}"

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
      echo "check-large-aws-gates: unknown option: $1" >&2
      usage
      ;;
    *)
      JSON_PATH="$1"
      ;;
  esac
  shift
done

if [[ -z "$JSON_PATH" ]]; then
  echo "check-large-aws-gates: JSON path required" >&2
  usage
fi

if [[ ! -f "$JSON_PATH" ]]; then
  echo "check-large-aws-gates: file not found: ${JSON_PATH}" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "check-large-aws-gates: jq required" >&2
  exit 1
fi

ENV="$(jq -r '.environment // empty' "$JSON_PATH")"
JSON_TIER="$(jq -r '.tier // empty' "$JSON_PATH")"
[[ -z "$TIER" && -n "$JSON_TIER" ]] && TIER="$JSON_TIER"
[[ -z "$TIER" ]] && TIER="l1"

case "$TIER" in
  l1|l2|l3) ;;
  *)
    echo "check-large-aws-gates: unknown tier: ${TIER}" >&2
    exit 1
    ;;
esac

RECALL_GATE="$(large_benchmark_tier_recall_gate "$TIER")"

if [[ "$ENFORCE_GATES" == "0" ]]; then
  echo "check-large-aws-gates: skipped (OPENPUFFER_BENCH_ENFORCE_GATES=0) ${JSON_PATH}"
  exit 0
fi

if [[ "$ENV" != "aws-s3" ]]; then
  echo "check-large-aws-gates: skipped (environment=${ENV:-<unset>}, AWS SLO gates apply only to aws-s3)"
  exit 0
fi

echo "check-large-aws-gates: ${JSON_PATH} tier=${TIER} (roundtrips<=${LARGE_BENCHMARK_AWS_STORAGE_ROUNDTRIPS_MAX}, recall@10>=${RECALL_GATE}, p50<${LARGE_BENCHMARK_AWS_P50_MS_MAX}ms)"

failures=()
while IFS= read -r line; do
  [[ -n "$line" ]] || continue
  failures+=("$line")
done < <(large_benchmark_aws_gate_failures "$JSON_PATH" "$RECALL_GATE" || true)

if [[ "${#failures[@]}" -gt 0 ]]; then
  echo "check-large-aws-gates: FAILED (${#failures[@]} gate(s)):" >&2
  for f in "${failures[@]}"; do
    echo "  - ${f}" >&2
  done
  echo "Set OPENPUFFER_BENCH_ENFORCE_GATES=0 to record without failing." >&2
  exit 1
fi

echo "check-large-aws-gates: OK (all AWS large-tier gates passed)"
exit 0