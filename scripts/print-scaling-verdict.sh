#!/usr/bin/env bash
# Print a single-paragraph operator verdict for openpuffer vs turbopuffer scaling.
# Uses committed benchmarks/results/op-scaling-*.json (offline; no MinIO bench).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec python3 "$ROOT/benchmarks/report/compare_op_scaling_to_tpuf.py" --verdict-only