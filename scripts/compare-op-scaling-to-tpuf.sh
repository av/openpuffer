#!/usr/bin/env bash
# Print openpuffer power-law extrapolation vs turbopuffer 10M official reference.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec python3 "$ROOT/benchmarks/report/compare_op_scaling_to_tpuf.py" "$@"