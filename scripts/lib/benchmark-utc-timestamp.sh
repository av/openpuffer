#!/usr/bin/env bash
# ISO8601 UTC timestamps for large-dataset benchmark JSON (Z suffix, no fractional seconds).
#
# Usage (after sourcing):
#   BENCHMARK_STARTED_AT="$(benchmark_utc_now)"
#   finished="$(benchmark_utc_now)"
#
benchmark_utc_now() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}