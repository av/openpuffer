# shellcheck shell=bash
# Verify namespace metadata is ready for cold-query benchmarking.
# Source from operator scripts; set NS_URL and NAMESPACE before calling.
# Do not execute directly.

# shellcheck disable=SC2168
[[ -n "${_NAMESPACE_META_LOADED:-}" ]] && return 0
_NAMESPACE_META_LOADED=1

# verify_namespace_meta
# Checks that the namespace has wal_commit_seq > 0, index_cursor == wal_commit_seq,
# and preferred_ann_version == 3.  Prints JSON meta on stdout; returns 1 on failure.
# Requires: NS_URL, NAMESPACE (set by the sourcing script).
verify_namespace_meta() {
  local meta
  meta="$(curl -sf "${NS_URL}" 2>/dev/null || true)"
  if [[ -z "$meta" ]]; then
    echo "namespace ${NAMESPACE} not found at ${NS_URL}" >&2
    return 1
  fi

  local cursor commit pref_ann
  cursor="$(echo "$meta" | jq -r '.index_cursor // 0')"
  commit="$(echo "$meta" | jq -r '.wal_commit_seq // 0')"
  pref_ann="$(echo "$meta" | jq -r '.preferred_ann_version // 2')"

  if [[ "$commit" == "0" ]]; then
    echo "namespace ${NAMESPACE}: wal_commit_seq is 0 (no ingest?)" >&2
    return 1
  fi
  if [[ "$cursor" != "$commit" ]]; then
    echo "namespace ${NAMESPACE}: index_cursor=${cursor} != wal_commit_seq=${commit}" >&2
    return 1
  fi
  if [[ "$pref_ann" != "3" ]]; then
    echo "namespace ${NAMESPACE}: preferred_ann_version=${pref_ann} (expected 3)" >&2
    return 1
  fi

  echo "$meta"
  return 0
}
