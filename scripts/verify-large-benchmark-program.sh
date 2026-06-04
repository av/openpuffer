#!/usr/bin/env bash
# Offline verification for the large-dataset benchmark program harness.
# One command for all local gates (no AWS/tpuf spend; MinIO G2 optional).
#
# Usage:
#   ./scripts/verify-large-benchmark-program.sh
#   ./scripts/verify-large-benchmark-program.sh --skip-l2-l3   # L1 + shared gates only
#   ./scripts/verify-large-benchmark-program.sh --with-g2       # adds MinIO Docker G2 (slow)
#   ./scripts/verify-large-benchmark-program.sh --skip-facts    # skip facts CLI (e.g. CI subset)
#
# Live comparison (G3–G5) still requires credentials:
#   ./scripts/run-large-benchmark-program.sh --tier l1
#
# See docs/PLAN_LARGE_DATASET_BENCHMARK.md § Program harness verification
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# shellcheck source=scripts/lib/benchmark-python-deps.sh
source "$ROOT/scripts/lib/benchmark-python-deps.sh"

SKIP_L2_L3=0
WITH_G2=0
SKIP_FACTS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-l2-l3) SKIP_L2_L3=1 ;;
    --with-g2) WITH_G2=1 ;;
    --skip-facts) SKIP_FACTS=1 ;;
    -h|--help)
      sed -n '2,16p' "$0"
      exit 0
      ;;
    *)
      echo "unknown argument: $1 (try --help)" >&2
      exit 1
      ;;
  esac
  shift
done

step() {
  echo ""
  echo "==> $*"
}

step "Python deps (benchmarks/requirements.txt)"
ensure_benchmark_python_deps "$ROOT"

step "pytest workloads (generate_synthetic)"
python3 -m pytest benchmarks/workloads/test_generate_synthetic.py -q

step "pytest tpuf driver (offline)"
python3 -m pytest benchmarks/tpuf_driver/test_run_benchmark.py -q

step "pytest id-overlap spotcheck (Phase 3.3)"
python3 -m pytest benchmarks/cross_check/test_id_overlap_spotcheck.py -q

step "render-report offline tests"
./scripts/test_render-report.sh
./scripts/test_render-report-measured.sh

step "shellcheck benchmark + report/gates scripts"
./scripts/test-shellcheck-benchmark-scripts.sh

step "benchmark JSON schema (fixtures + *.example.json)"
./scripts/validate-benchmark-json.sh

step "benchmark results git policy (tracked + staged)"
./scripts/check-benchmark-artifacts.sh
./scripts/test_check-benchmark-artifacts.sh

step "ingest/bench JSON schema tests"
./scripts/test_ingest-timing-schema.sh
./scripts/test_ingest-large-retry.sh
./scripts/test_bench-large-secondary-schema.sh
./scripts/test_large-benchmark-serve-ready.sh
./scripts/test_estimate-large-benchmark-cost.sh

step "synthetic_workload_gate (fixture vectors + recall_defaults)"
cargo test --test synthetic_workload_gate -q

step "L1 harness dry-run (ingest, bench, G3, G4, program, overlap, tpuf driver)"
./scripts/ingest-large.sh --tier l1 --dry-run >/dev/null
./scripts/bench-large.sh --tier l1 --dry-run >/dev/null
./scripts/run-aws-large-benchmark.sh --tier l1 --dry-run >/dev/null
./scripts/run-tpuf-large-benchmark.sh --tier l1 --dry-run --skip-g2 >/dev/null
python3 benchmarks/tpuf_driver/run_benchmark.py --tier l1 --dry-run >/dev/null
./scripts/run-id-overlap-spotcheck.sh --tier l1 --dry-run >/dev/null
OPENPUFFER_REPORT_OUTPUT="/tmp/openpuffer-verify-program-l1.md" \
  OPENPUFFER_REPORT_DATE=2099-06-04 \
  ./scripts/run-large-benchmark-program.sh --tier l1 --dry-run --skip-g2 >/dev/null

if [[ "$SKIP_L2_L3" != "1" ]]; then
  step "L2/L3 harness dry-run (all tiers)"
  ./scripts/test_l2-l3-harness-dry-run.sh
else
  echo ""
  echo "==> skipping L2/L3 harness dry-run (--skip-l2-l3)"
fi

if [[ "$WITH_G2" == "1" ]]; then
  step "G2 MinIO correctness gates (Docker; slow)"
  ./scripts/run-minio-correctness-gates.sh
else
  echo ""
  echo "==> skipping G2 MinIO gates (pass --with-g2 for Docker parity with CI)"
fi

if [[ "$SKIP_FACTS" != "1" ]]; then
  if ! command -v facts >/dev/null 2>&1; then
    echo "facts CLI not found; install from https://github.com/av/facts or pass --skip-facts" >&2
    exit 1
  fi
  step "facts check (bench-large)"
  facts check --tags bench-large
  step "facts check (bench-tpuf)"
  facts check --tags bench-tpuf
else
  echo ""
  echo "==> skipping facts check (--skip-facts)"
fi

echo ""
echo "verify-large-benchmark-program: OK (offline harness complete)"
echo "  Live G3–G5: ./scripts/run-large-benchmark-program.sh --tier l1  # needs OPENPUFFER_S3_* + TURBOPUFFER_API_KEY"