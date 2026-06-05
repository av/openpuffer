#!/usr/bin/env bash
# Fast offline gate: openpuffer vs turbopuffer scaling comparison (committed JSON only).
# No Docker, no MinIO bench — does not run run-op-scaling-benchmark.sh / 100k ingest.
#
# Usage:
#   ./scripts/verify-op-scaling-comparison.sh
#
# Refresh measured tiers (operator; slow):
#   make bench-op-scaling && make bench-compare-tpuf
#
# See benchmarks/README.md § openpuffer vs turbopuffer scaling
set -euo pipefail

usage() {
  cat <<'EOF'
verify-op-scaling-comparison.sh — offline CI gate for op vs tpuf scaling artifacts.

Purpose:
  Validate committed op-scaling JSON (schema, pytest, synthetic128), run compare smoke
  test, and validate scaling-comparison-summary.json — no Docker, no MinIO bench.

Usage:
  ./scripts/verify-op-scaling-comparison.sh
  ./scripts/verify-op-scaling-comparison.sh -h|--help
  make bench-verify-op-scaling

Environment:
  (none required; uses benchmarks/requirements.txt via ensure_benchmark_python_deps)

Does not run:
  run-op-scaling-benchmark.sh / 100k ingest (refresh: make bench-op-scaling)

Input tiers (committed benchmarks/results/):
  op-scaling-*.json, tpuf-official-reference.json, scaling-comparison-summary.json

Output:
  Exit 0 + "verify-op-scaling-comparison: OK" on success; no new artifacts.

Quickstart:
  benchmarks/SCALING_VS_TPUF_QUICKSTART.md  (step 3 offline gate)
EOF
}

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

# shellcheck source=scripts/lib/benchmark-python-deps.sh
source "$ROOT/scripts/lib/benchmark-python-deps.sh"

step() {
  echo ""
  echo "==> $*"
}

step "Python 3.11+ (benchmark harness)"
ensure_benchmark_python_version

step "Python deps (benchmarks/requirements.txt)"
ensure_benchmark_python_deps "$ROOT"

step "op-scaling JSON schema (committed MinIO tiers)"
./scripts/test_validate-op-scaling-json.sh

if [[ -f benchmarks/results/op-scaling-10k-synthetic128.json ]]; then
  ./scripts/validate-benchmark-json.sh benchmarks/results/op-scaling-10k-synthetic128.json
fi

step "pytest op_scaling schema (all op-scaling-*.json)"
python3 -m pytest benchmarks/report/test_op_scaling_schema.py -q

step "compare op-scaling to tpuf (committed JSON smoke)"
./scripts/test_compare-op-scaling-to-tpuf.sh

step "scaling-comparison-summary.json schema"
./scripts/validate-benchmark-json.sh benchmarks/results/scaling-comparison-summary.json

echo ""
echo "verify-op-scaling-comparison: OK (offline; no MinIO bench)"