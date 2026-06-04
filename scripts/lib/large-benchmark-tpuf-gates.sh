# shellcheck shell=bash
# Turbopuffer large-tier SLO gates for tpuf-*.json (shared by run_benchmark.py and check-tpuf-gates).
# Source from benchmark scripts; do not execute directly.

# shellcheck disable=SC2168
[[ -n "${_LARGE_BENCHMARK_TPUF_GATES_LOADED:-}" ]] && return 0
_LARGE_BENCHMARK_TPUF_GATES_LOADED=1

_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib/large-benchmark-exit-codes.sh
source "${_LIB_DIR}/large-benchmark-exit-codes.sh"

readonly LARGE_BENCHMARK_TPUF_COLD_QUERY_RUNS=7

large_benchmark_tpuf_tier_recall_gate() {
  local tier="${1:-l1}"
  case "$tier" in
    l1|l2|l3) echo "0.85" ;;
    *)
      echo "0.85"
      ;;
  esac
}

# Echo one human-readable failure per line; return 0 if all gates pass.
large_benchmark_tpuf_gate_failures() {
  local json_path="$1"
  local recall_gate="${2:-0.85}"

  if ! command -v jq >/dev/null 2>&1; then
    echo "jq required for tpuf gate check" >&2
    return 2
  fi
  if [[ ! -f "$json_path" ]]; then
    echo "missing JSON: ${json_path}" >&2
    return 2
  fi

  jq -r \
    --argjson recall_gate "$recall_gate" \
    --argjson cold_runs "$LARGE_BENCHMARK_TPUF_COLD_QUERY_RUNS" \
    '
    def fail($msg): $msg;
    (
      if (.environment | type) != "string" or (.environment | startswith("turbopuffer:") | not) then
        fail("environment must start with turbopuffer: (got \(.environment // "null"))")
      else empty end,
      if (.tpuf_region | type) != "string" or (.tpuf_region | length) == 0 then
        fail("tpuf_region must be a non-empty string")
      elif .environment != ("turbopuffer:" + .tpuf_region) then
        fail("environment must be turbopuffer:\(.tpuf_region) (got \(.environment))")
      else empty end,
      if (.cold_query_runs | tonumber? // null) != $cold_runs then
        fail("cold_query_runs must be \($cold_runs) (got \(.cold_query_runs // "null"))")
      else empty end,
      if .index_up_to_date != true then
        fail("index_up_to_date must be true")
      else empty end,
      if (.recall_at_10 | tonumber? // null) == null then
        fail("recall_at_10 missing or non-numeric")
      elif (.recall_at_10 | tonumber) < ($recall_gate | tonumber) then
        fail("recall_at_10 must be >= \($recall_gate) (got \(.recall_at_10))")
      else empty end
    )
    ' "$json_path"
}

# Exit 0 pass, 1 fail, 2 usage/preflight (missing file/jq).
large_benchmark_check_tpuf_gates_file() {
  local json_path="$1"
  local recall_gate="${2:-0.85}"
  local -a failures=()

  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    failures+=("$line")
  done < <(large_benchmark_tpuf_gate_failures "$json_path" "$recall_gate" || true)

  if [[ "${#failures[@]}" -gt 0 ]]; then
    printf '%s\n' "${failures[@]}"
    return 1
  fi
  return 0
}