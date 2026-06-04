# shellcheck shell=bash
# AWS large-tier SLO gates for large-aws-*.json (shared by bench-large and check-large-aws-gates).
# Source from benchmark scripts; do not execute directly.

# shellcheck disable=SC2168
[[ -n "${_LARGE_BENCHMARK_AWS_GATES_LOADED:-}" ]] && return 0
_LARGE_BENCHMARK_AWS_GATES_LOADED=1

_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib/large-benchmark-exit-codes.sh
source "${_LIB_DIR}/large-benchmark-exit-codes.sh"

readonly LARGE_BENCHMARK_AWS_P50_MS_MAX=600
readonly LARGE_BENCHMARK_AWS_STORAGE_ROUNDTRIPS_MAX=4

large_benchmark_tier_recall_gate() {
  local tier="${1:-l1}"
  case "$tier" in
    l1|l2|l3) echo "0.85" ;;
    *)
      echo "0.85"
      ;;
  esac
}

# Echo one human-readable failure per line; return 0 if all gates pass.
large_benchmark_aws_gate_failures() {
  local json_path="$1"
  local recall_gate="${2:-0.85}"

  if ! command -v jq >/dev/null 2>&1; then
    echo "jq required for AWS gate check" >&2
    return 2
  fi
  if [[ ! -f "$json_path" ]]; then
    echo "missing JSON: ${json_path}" >&2
    return 2
  fi

  jq -r \
    --argjson recall_gate "$recall_gate" \
    --argjson p50_max "$LARGE_BENCHMARK_AWS_P50_MS_MAX" \
    --argjson rt_max "$LARGE_BENCHMARK_AWS_STORAGE_ROUNDTRIPS_MAX" \
    '
    def fail($msg): $msg;
    (
      if (.preferred_ann_version | tonumber? // null) != 3 then
        fail("preferred_ann_version must be 3 (got \(.preferred_ann_version // "null"))")
      else empty end,
      if .index_cursor_eq_wal_commit_seq != true then
        fail("index_cursor_eq_wal_commit_seq must be true")
      else empty end,
      if (.storage_roundtrips | tonumber? // null) == null then
        fail("storage_roundtrips missing or non-numeric")
      elif (.storage_roundtrips | tonumber) > $rt_max then
        fail("storage_roundtrips must be <= \($rt_max) (got \(.storage_roundtrips))")
      else empty end,
      if (.recall_at_10 | tonumber? // null) == null then
        fail("recall_at_10 missing or non-numeric")
      elif (.recall_at_10 | tonumber) < ($recall_gate | tonumber) then
        fail("recall_at_10 must be >= \($recall_gate) (got \(.recall_at_10))")
      else empty end,
      if (.p50_query_latency_ms | tonumber? // null) == null then
        fail("p50_query_latency_ms missing or non-numeric")
      elif (.p50_query_latency_ms | tonumber) >= ($p50_max | tonumber) then
        fail("p50_query_latency_ms must be < \($p50_max) ms (got \(.p50_query_latency_ms))")
      else empty end
    )
    ' "$json_path"
}

# Exit 0 pass, 1 fail, 2 usage/preflight (missing file/jq).
large_benchmark_check_aws_gates_file() {
  local json_path="$1"
  local recall_gate="${2:-0.85}"
  local -a failures=()

  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    failures+=("$line")
  done < <(large_benchmark_aws_gate_failures "$json_path" "$recall_gate" || true)

  if [[ "${#failures[@]}" -gt 0 ]]; then
    printf '%s\n' "${failures[@]}"
    return 1
  fi
  return 0
}