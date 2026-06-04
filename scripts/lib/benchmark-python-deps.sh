# Shared Python dependency helpers for the large-dataset benchmark harness.
# Source from bash scripts; do not execute directly.

# Match CI (actions/setup-python) and benchmarks/requirements.txt.
BENCHMARK_PYTHON_MIN_MAJOR=3
BENCHMARK_PYTHON_MIN_MINOR=11

benchmark_python_req_file() {
  local root="${1:?}"
  echo "${root}/benchmarks/requirements.txt"
}

# Fail fast when python3 is missing or below BENCHMARK_PYTHON_MIN_*.
ensure_benchmark_python_version() {
  if ! command -v python3 >/dev/null 2>&1; then
    echo "ensure_benchmark_python_version: python3 not found in PATH" >&2
    return 1
  fi
  if ! python3 -c "
import sys
maj, min_ = sys.version_info[:2]
need = (${BENCHMARK_PYTHON_MIN_MAJOR}, ${BENCHMARK_PYTHON_MIN_MINOR})
if (maj, min_) < need:
    print(
        f'Python {maj}.{min_} < required {need[0]}.{need[1]}+ '
        f'(CI uses setup-python {need[0]}.{need[1]}; ./scripts/install-benchmark-python-deps.sh)',
        file=sys.stderr,
    )
    raise SystemExit(1)
"; then
    return 1
  fi
}

# Install benchmarks/requirements.txt when any listed import is missing.
ensure_benchmark_python_deps() {
  local root="${1:?}"
  local req
  ensure_benchmark_python_version
  req="$(benchmark_python_req_file "$root")"
  if [[ ! -f "$req" ]]; then
    echo "ensure_benchmark_python_deps: missing ${req}" >&2
    return 1
  fi

  local need=0
  for mod in pytest jsonschema httpx turbopuffer; do
    if ! python3 -c "import ${mod}" >/dev/null 2>&1; then
      need=1
      break
    fi
  done
  if [[ "$need" -eq 0 ]]; then
    return 0
  fi

  echo "ensure_benchmark_python_deps: installing from ${req}…" >&2
  python3 -m pip install --quiet --upgrade pip
  python3 -m pip install -q -r "$req"
}