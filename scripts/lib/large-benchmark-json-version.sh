# Large-dataset harness result JSON schema_version (not workload manifest schema_version).
# Canonical value: benchmarks/report/LARGE_BENCHMARK_JSON_SCHEMA_VERSION
large_benchmark_json_schema_version() {
  if [[ -z "${LARGE_BENCHMARK_JSON_SCHEMA_VERSION:-}" ]]; then
    local root="${LARGE_BENCHMARK_JSON_ROOT:-${LARGE_PREFLIGHT_ROOT:-${ROOT:-}}}"
    [[ -n "$root" ]] || {
      echo "large_benchmark_json_schema_version: ROOT not set" >&2
      return 1
    }
    LARGE_BENCHMARK_JSON_SCHEMA_VERSION="$(
      tr -d '\n' <"${root}/benchmarks/report/LARGE_BENCHMARK_JSON_SCHEMA_VERSION"
    )"
    export LARGE_BENCHMARK_JSON_SCHEMA_VERSION
  fi
  printf '%s' "$LARGE_BENCHMARK_JSON_SCHEMA_VERSION"
}