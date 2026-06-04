#!/usr/bin/env bash
# Offline tests for scripts/lib/large-benchmark-serve-ready.sh (mock HTTP, no openpuffer binary).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# shellcheck source=scripts/lib/large-benchmark-serve-ready.sh
source "$ROOT/scripts/lib/large-benchmark-serve-ready.sh"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

MOCK_PID=""
MOCK_PORT=""
cleanup() {
  if [[ -n "$MOCK_PID" ]]; then
    kill "$MOCK_PID" 2>/dev/null || true
    wait "$MOCK_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

start_mock_server() {
  local mode="$1"
  MOCK_PORT="$(
    python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
  )"
  python3 - "$MOCK_PORT" "$mode" <<'PY' &
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

port = int(sys.argv[1])
mode = sys.argv[2]

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        ok = False
        if mode == "health" and self.path == "/health":
            ok = True
        elif mode == "ready" and self.path == "/v1/ready":
            ok = True
        elif mode == "both" and self.path in ("/health", "/v1/ready"):
            ok = True
        if ok:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        else:
            self.send_response(404)
            self.end_headers()
    def log_message(self, *_args):
        pass

HTTPServer(("127.0.0.1", port), Handler).serve_forever()
PY
  MOCK_PID=$!
  local deadline=$(( $(date +%s) + 10 ))
  while [[ $(date +%s) -lt $deadline ]]; do
    if curl -sf "http://127.0.0.1:${MOCK_PORT}/health" >/dev/null 2>&1 \
      || curl -sf "http://127.0.0.1:${MOCK_PORT}/v1/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  fail "mock server did not start on port ${MOCK_PORT}"
}

[[ "$(OPENPUFFER_SERVE_READY_TIMEOUT_SEC=99 large_benchmark_serve_ready_timeout_sec)" == "99" ]] \
  || fail "timeout env override"
[[ "$(large_benchmark_serve_ready_timeout_sec)" == "120" ]] \
  || fail "default timeout"

start_mock_server health
large_benchmark_wait_for_serve_ready "http://127.0.0.1:${MOCK_PORT}" "" \
  || fail "expected /health mock to become ready"
kill "$MOCK_PID" 2>/dev/null || true
wait "$MOCK_PID" 2>/dev/null || true
MOCK_PID=""

start_mock_server ready
large_benchmark_wait_for_serve_ready "http://127.0.0.1:${MOCK_PORT}" "" \
  || fail "expected /v1/ready mock to become ready"
kill "$MOCK_PID" 2>/dev/null || true
wait "$MOCK_PID" 2>/dev/null || true
MOCK_PID=""

# Failure path: closed port, short timeout, message mentions probes.
DEAD_PORT="$(
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
)"
if OPENPUFFER_SERVE_READY_TIMEOUT_SEC=2 \
  large_benchmark_wait_for_serve_ready "http://127.0.0.1:${DEAD_PORT}" "" 2>"$ROOT/.tmp-serve-ready-fail.txt"; then
  fail "expected timeout against closed port"
fi
grep -q '/health' "$ROOT/.tmp-serve-ready-fail.txt" \
  || fail "failure message missing /health"
grep -q '/v1/ready' "$ROOT/.tmp-serve-ready-fail.txt" \
  || fail "failure message missing /v1/ready"
grep -q 'within 2s' "$ROOT/.tmp-serve-ready-fail.txt" \
  || fail "failure message missing timeout"
rm -f "$ROOT/.tmp-serve-ready-fail.txt"

if command -v shellcheck >/dev/null 2>&1; then
  shellcheck -x scripts/lib/large-benchmark-serve-ready.sh
fi

grep -q 'large-benchmark-serve-ready.sh' scripts/ingest-large.sh \
  || fail 'ingest-large.sh must source serve-ready lib'
grep -q 'large-benchmark-serve-ready.sh' scripts/bench-large.sh \
  || fail 'bench-large.sh must source serve-ready lib'
grep -q 'serve_ready_timeout' scripts/ingest-large.sh \
  || fail 'ingest-large dry-run should mention serve_ready_timeout'
grep -q 'serve_ready_timeout' scripts/bench-large.sh \
  || fail 'bench-large dry-run should mention serve_ready_timeout'

./scripts/ingest-large.sh --dry-run >/dev/null
./scripts/bench-large.sh --dry-run >/dev/null

echo "large-benchmark-serve-ready tests OK"