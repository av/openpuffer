# shellcheck shell=bash
# Standard exit codes for large-dataset benchmark shell harnesses.
# Source from operator scripts; see benchmarks/README.md § Exit codes.
# Do not execute directly.

[[ -n "${_LARGE_BENCHMARK_EXIT_CODES_LOADED:-}" ]] && return 0
_LARGE_BENCHMARK_EXIT_CODES_LOADED=1

# Success (documented for operators; callers use plain exit 0)
# shellcheck disable=SC2034
readonly LARGE_BENCHMARK_EXIT_OK=0

# Operator / environment preflight (credentials, region, workload, wrong storage backend)
readonly LARGE_BENCHMARK_EXIT_PREFLIGHT=1

# openpuffer serve did not pass /v1/ready or /health within OPENPUFFER_SERVE_READY_TIMEOUT_SEC
readonly LARGE_BENCHMARK_EXIT_SERVE_TIMEOUT=2

# Indexer did not reach index_cursor == wal_commit_seq (and ann v3) within ingest/bench timeout
readonly LARGE_BENCHMARK_EXIT_INDEX_TIMEOUT=3

# CLI usage (unknown flags); preflight entrypoints only — other scripts may still use 1
readonly LARGE_BENCHMARK_EXIT_USAGE=64

large_benchmark_exit() {
  local code="${1:?large_benchmark_exit: code required}"
  shift || true
  if [[ $# -gt 0 ]]; then
    echo "$*" >&2
  fi
  exit "$code"
}

large_benchmark_exit_preflight() {
  large_benchmark_exit "$LARGE_BENCHMARK_EXIT_PREFLIGHT" "$@"
}

large_benchmark_exit_serve_timeout() {
  large_benchmark_exit "$LARGE_BENCHMARK_EXIT_SERVE_TIMEOUT" "$@"
}

large_benchmark_exit_index_timeout() {
  large_benchmark_exit "$LARGE_BENCHMARK_EXIT_INDEX_TIMEOUT" "$@"
}

large_benchmark_exit_usage() {
  large_benchmark_exit "$LARGE_BENCHMARK_EXIT_USAGE" "$@"
}