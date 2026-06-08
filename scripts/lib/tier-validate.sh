# shellcheck shell=bash
# Validate a benchmark tier value (l1, l2, or l3).
# Source from operator scripts; call validate_tier after argument parsing.
# Do not execute directly.

# shellcheck disable=SC2168
[[ -n "${_TIER_VALIDATE_LOADED:-}" ]] && return 0
_TIER_VALIDATE_LOADED=1

# validate_tier TIER [CALLER]
# Exits 1 with a message to stderr when TIER is not l1, l2, or l3.
# CALLER is an optional script name prefix for the error message.
validate_tier() {
  local tier="${1:?validate_tier: tier value required}"
  local caller="${2:-}"
  case "$tier" in
    l1|l2|l3) return 0 ;;
    *)
      local prefix=""
      [[ -n "$caller" ]] && prefix="${caller}: "
      echo "${prefix}unknown tier: ${tier} (use l1, l2, or l3)" >&2
      exit 1
      ;;
  esac
}

# tier_defaults TIER
# Sets TIER_DOCS and TIER_WORKLOAD for the given tier.
# Exits 1 if the tier is invalid (delegates to validate_tier).
tier_defaults() {
  local tier="${1:?tier_defaults: tier value required}"
  validate_tier "$tier" "tier_defaults"
  case "$tier" in
    l1) TIER_DOCS=100000;  TIER_WORKLOAD="benchmarks/workloads/synthetic-128/l1-100k" ;;
    l2) TIER_DOCS=500000;  TIER_WORKLOAD="benchmarks/workloads/synthetic-128/l2-500k" ;;
    l3) TIER_DOCS=1000000; TIER_WORKLOAD="benchmarks/workloads/synthetic-128/l3-1m" ;;
  esac
}
