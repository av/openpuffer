# shellcheck shell=bash
# Poll openpuffer serve until HTTP readiness before upsert/query (ingest-large, bench-large).

# shellcheck source=scripts/lib/large-benchmark-exit-codes.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/large-benchmark-exit-codes.sh"
# Probes GET /health (shallow liveness) and GET /v1/ready (S3 HeadBucket + openpuffer/ read).
# Either returning HTTP 2xx is sufficient; prefer /v1/ready when you need storage-backed traffic.
# Source from ingest-large.sh / bench-large.sh — do not execute directly.

# Last failed probe (for diagnostics).
SERVE_READY_LAST_ENDPOINT=""
SERVE_READY_LAST_HTTP=""
SERVE_READY_LAST_CURL=""

large_benchmark_serve_ready_timeout_sec() {
  echo "${OPENPUFFER_SERVE_READY_TIMEOUT_SEC:-120}"
}

large_benchmark_serve_ready_poll_interval_sec() {
  echo "${OPENPUFFER_SERVE_READY_POLL_SEC:-0.5}"
}

# Returns 0 when url responds with HTTP 2xx.
large_benchmark_serve_probe_url() {
  local url="$1"
  local http curl_exit=0
  http="$(
    curl -sS -o /dev/null -w '%{http_code}' \
      --connect-timeout 2 \
      --max-time 5 \
      "$url" 2>/dev/null
  )" || curl_exit=$?
  if [[ -n "$http" && "$http" =~ ^2 ]]; then
    SERVE_READY_LAST_ENDPOINT="$url"
    SERVE_READY_LAST_HTTP="$http"
    SERVE_READY_LAST_CURL=0
    return 0
  fi
  SERVE_READY_LAST_ENDPOINT="$url"
  SERVE_READY_LAST_HTTP="${http:-000}"
  SERVE_READY_LAST_CURL="$curl_exit"
  return 1
}

# /v1/ready first (S3-backed readiness), then shallow /health.
large_benchmark_serve_is_ready() {
  local base_url="${1%/}"
  if large_benchmark_serve_probe_url "${base_url}/v1/ready"; then
    return 0
  fi
  if large_benchmark_serve_probe_url "${base_url}/health"; then
    return 0
  fi
  return 1
}

large_benchmark_serve_ready_pid_status() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    echo "serve_pid=(not tracked; external or OPENPUFFER_*_SKIP_SERVE=1)"
    return 0
  fi
  if kill -0 "$pid" 2>/dev/null; then
    echo "serve_pid=${pid} still running (process alive; HTTP not accepting yet)"
    return 0
  fi
  local exit_code="unknown"
  if wait "$pid" 2>/dev/null; then
    exit_code=$?
  fi
  echo "serve_pid=${pid} exited (code=${exit_code}); check serve logs / S3 env / port bind"
}

large_benchmark_serve_ready_print_failure() {
  local base_url="${1%/}"
  local serve_pid="${2:-}"
  local timeout_sec="${3:-$(large_benchmark_serve_ready_timeout_sec)}"
  {
    echo "serve did not become ready within ${timeout_sec}s at ${base_url}"
    echo "  probed: GET ${base_url}/health OR GET ${base_url}/v1/ready (need HTTP 2xx)"
    echo "  last_probe: ${SERVE_READY_LAST_ENDPOINT:-<none>} http=${SERVE_READY_LAST_HTTP:-?} curl_exit=${SERVE_READY_LAST_CURL:-?}"
    large_benchmark_serve_ready_pid_status "$serve_pid"
    echo "  hints: port in use?; cargo build failed?; wrong OPENPUFFER_S3_* (serve may exit); raise OPENPUFFER_SERVE_READY_TIMEOUT_SEC"
    echo "  env: OPENPUFFER_SERVE_READY_TIMEOUT_SEC OPENPUFFER_SERVE_READY_POLL_SEC"
  } >&2
}

# Wait until serve accepts traffic. Optional second arg: background serve PID for diagnostics.
large_benchmark_wait_for_serve_ready() {
  local base_url="${1%/}"
  local serve_pid="${2:-}"
  local timeout_sec poll_sec start end
  timeout_sec="$(large_benchmark_serve_ready_timeout_sec)"
  poll_sec="$(large_benchmark_serve_ready_poll_interval_sec)"

  if [[ -z "$base_url" ]]; then
    echo "large_benchmark_wait_for_serve_ready: base_url required" >&2
    return 1
  fi

  echo "Waiting for serve readiness (${timeout_sec}s timeout) at ${base_url} …" >&2
  start=$(date +%s)
  end=$((start + timeout_sec))

  while [[ $(date +%s) -lt $end ]]; do
    if large_benchmark_serve_is_ready "$base_url"; then
      echo "serve ready (${SERVE_READY_LAST_ENDPOINT} http=${SERVE_READY_LAST_HTTP})" >&2
      return 0
    fi
    sleep "$poll_sec"
  done

  large_benchmark_serve_ready_print_failure "$base_url" "$serve_pid" "$timeout_sec"
  return 1
}

# Back-compat alias used by ingest-large / bench-large / bench-1m.
# BASE_URL and SERVE_PID are defined by the sourcing script.
wait_for_health() {
  # shellcheck disable=SC2153
  large_benchmark_wait_for_serve_ready "${BASE_URL}" "${SERVE_PID:-}" \
    || large_benchmark_exit "$LARGE_BENCHMARK_EXIT_SERVE_TIMEOUT"
}