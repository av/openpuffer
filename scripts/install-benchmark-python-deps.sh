#!/usr/bin/env bash
# Install consolidated Python deps for benchmarks/ (workloads, tpuf_driver, cross_check, JSON Schema).
#
# Usage:
#   ./scripts/install-benchmark-python-deps.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# shellcheck source=scripts/lib/benchmark-python-deps.sh
source "$ROOT/scripts/lib/benchmark-python-deps.sh"

ensure_benchmark_python_version

req="$(benchmark_python_req_file "$ROOT")"
[[ -f "$req" ]] || {
  echo "install-benchmark-python-deps: missing ${req}" >&2
  exit 1
}

python3 -m pip install --upgrade pip
python3 -m pip install -r "$req"
echo "install-benchmark-python-deps: OK (${req})"