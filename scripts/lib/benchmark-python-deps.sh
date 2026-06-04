# Shared Python dependency helpers for the large-dataset benchmark harness.
# Source from bash scripts; do not execute directly.

benchmark_python_req_file() {
  local root="${1:?}"
  echo "${root}/benchmarks/requirements.txt"
}

# Install benchmarks/requirements.txt when any listed import is missing.
ensure_benchmark_python_deps() {
  local root="${1:?}"
  local req
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