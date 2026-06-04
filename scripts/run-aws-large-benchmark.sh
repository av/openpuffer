#!/usr/bin/env bash
# G3 operator wrapper: MinIO G2 preflight → AWS S3 env check → ingest-large → bench-large.
# Produces benchmarks/results/large-aws-{tier}.json for Phase 7 / COMPARISON.md (AWS only).
#
# Usage:
#   ./scripts/run-aws-large-benchmark.sh --tier l1
#   ./scripts/run-aws-large-benchmark.sh --tier l3
#   ./scripts/run-aws-large-benchmark.sh --preflight-only   # G2 + AWS env, no ingest
#   ./scripts/run-aws-large-benchmark.sh --dry-run          # print plan + env checklist
#   ./scripts/run-aws-large-benchmark.sh --skip-g2          # skip MinIO correctness subset
#   ./scripts/run-aws-large-benchmark.sh --ingest-only
#   ./scripts/run-aws-large-benchmark.sh --bench-only       # namespace must exist
#
# Environment: see large_preflight_print_aws_operator_env in scripts/lib/large-benchmark-preflight.sh
# and docs/BENCHMARKS.md § large-dataset program operator runbook.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export LARGE_PREFLIGHT_ROOT="$ROOT"
# shellcheck source=scripts/lib/large-benchmark-preflight.sh
source "$ROOT/scripts/lib/large-benchmark-preflight.sh"
# shellcheck source=scripts/lib/estimate-large-benchmark-cost.sh
source "$ROOT/scripts/lib/estimate-large-benchmark-cost.sh"

TIER="${OPENPUFFER_BENCH_TIER:-l1}"
DRY_RUN=0
PREFLIGHT_ONLY=0
SKIP_G2=0
INGEST_ONLY=0
BENCH_ONLY=0
SKIP_INGEST=0
SKIP_BENCH=0

for arg in "$@"; do
  case "$arg" in
    --dry-run|-n) DRY_RUN=1 ;;
    --preflight-only) PREFLIGHT_ONLY=1 ;;
    --skip-g2) SKIP_G2=1 ;;
    --ingest-only) INGEST_ONLY=1; SKIP_BENCH=1 ;;
    --bench-only) BENCH_ONLY=1; SKIP_INGEST=1 ;;
    --tier=*) TIER="${arg#*=}" ;;
    --tier) shift; TIER="${1:?--tier requires l1|l2|l3}" ;;
    -h|--help)
      sed -n '2,22p' "$0"
      large_preflight_print_aws_operator_env
      exit 0
      ;;
  esac
done

case "$TIER" in
  l1|l2|l3) ;;
  *)
    echo "unknown tier: ${TIER} (use l1, l2, or l3)" >&2
    exit 1
    ;;
esac

RESULTS_DEFAULT="$ROOT/benchmarks/results/large-aws-${TIER}.json"
RESULTS="${OPENPUFFER_BENCH_RESULTS:-$RESULTS_DEFAULT}"

run_plan_dry() {
  echo "run-aws-large-benchmark dry-run OK"
  echo "  tier=${TIER}"
  echo "  results=${RESULTS}"
  echo "  ingest_only=${INGEST_ONLY} bench_only=${BENCH_ONLY}"
  echo "  steps: $([[ "$SKIP_G2" == 1 ]] && echo 'skip-g2' || echo 'g2-subset') → aws-preflight → ingest-large → bench-large"
  echo "  index_timeout_default=$(large_preflight_tier_index_timeout_sec "$TIER")s (override: OPENPUFFER_INGEST_INDEX_TIMEOUT_SEC / OPENPUFFER_BENCH_INDEX_TIMEOUT_SEC)"
  large_preflight_aws_time_estimate "$TIER"
  large_benchmark_cost_print "$TIER" 0 aws
  large_preflight_print_aws_operator_env
  if [[ -n "${OPENPUFFER_S3_BUCKET:-}" ]]; then
    echo "  OPENPUFFER_S3_BUCKET=${OPENPUFFER_S3_BUCKET} (set)"
    echo "  detected_environment=$(large_preflight_detect_environment)"
  else
    echo "  OPENPUFFER_S3_* unset (required for live run)"
  fi
  exit 0
}

[[ "$DRY_RUN" == "1" ]] && run_plan_dry

large_preflight_toolchain
large_preflight_ann_version
large_preflight_validate_tier_workload "$TIER" "$ROOT"

if [[ "$SKIP_G2" != "1" ]]; then
  large_preflight_run_g2_subset "$ROOT"
fi

if [[ -x "$ROOT/scripts/preflight-aws-ec2.sh" ]]; then
  "$ROOT/scripts/preflight-aws-ec2.sh" || \
    large_benchmark_exit_preflight "preflight-aws-ec2 failed (region/metadata/S3); fix before live G3"
fi

large_preflight_validate_s3_env
ENV_DETECTED="$(large_preflight_detect_environment)"
if [[ "$ENV_DETECTED" != "aws-s3" ]]; then
  {
    echo "preflight: OPENPUFFER_S3_ENDPOINT does not look like AWS (${ENV_DETECTED})" >&2
    echo "  G3 comparison artifacts require aws-s3. For MinIO schema validation use:" >&2
    echo "    ./scripts/run-minio-large-schema-example.sh --tier ${TIER}" >&2
  }
  large_benchmark_exit_preflight
fi
export OPENPUFFER_BENCH_ENVIRONMENT=aws-s3

large_preflight_s3_head_bucket
large_preflight_guard_aws_results_path "$ENV_DETECTED" "$RESULTS"

echo "run-aws-large-benchmark: tier=${TIER} environment=${ENV_DETECTED} results=${RESULTS}"
if [[ -n "${OPENPUFFER_BENCH_HOST_LABEL:-}" ]]; then
  echo "  host=${OPENPUFFER_BENCH_HOST_LABEL}"
fi

if [[ "$PREFLIGHT_ONLY" == "1" ]]; then
  echo "preflight-only: OK (G2 subset + AWS env + workload). Run without --preflight-only to ingest+bench."
  exit 0
fi

if [[ "$SKIP_INGEST" != "1" ]]; then
  echo "==> ingest-large --tier ${TIER}"
  ./scripts/ingest-large.sh --tier "$TIER"
fi

if [[ "$SKIP_BENCH" != "1" ]]; then
  echo "==> bench-large --tier ${TIER}"
  export OPENPUFFER_BENCH_RESULTS="$RESULTS"
  export OPENPUFFER_BENCH_ENFORCE_GATES="${OPENPUFFER_BENCH_ENFORCE_GATES:-1}"
  ./scripts/bench-large.sh --tier "$TIER"
  echo "==> check-large-aws-gates ${RESULTS}"
  export OPENPUFFER_BENCH_ENFORCE_GATES="${OPENPUFFER_BENCH_ENFORCE_GATES:-1}"
  ./scripts/check-large-aws-gates.sh --tier "$TIER" "$RESULTS"
fi

echo "G3 complete: ${RESULTS}"
echo "Next: ./scripts/run-tpuf-large-benchmark.sh --tier ${TIER}  # G4"
echo "      ./scripts/render-report.sh --date $(date +%F)"