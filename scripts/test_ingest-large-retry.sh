#!/usr/bin/env bash
# Unit tests for ingest-large transient retry classification and backoff (no live HTTP).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# shellcheck source=scripts/lib/ingest-large-retry.sh
source "$ROOT/scripts/lib/ingest-large-retry.sh"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_transient() {
  local curl_exit="$1" http="$2"
  ingest_large_is_transient_failure "$curl_exit" "$http" || fail "expected transient curl=${curl_exit} http=${http}"
}

assert_permanent() {
  local curl_exit="$1" http="$2"
  if ingest_large_is_transient_failure "$curl_exit" "$http"; then
    fail "expected permanent curl=${curl_exit} http=${http}"
  fi
}

# Transient: connection reset, timeouts, 5xx, 429, http 000
assert_transient 56 000
assert_transient 7 000
assert_transient 28 000
assert_transient 0 500
assert_transient 0 502
assert_transient 0 503
assert_transient 0 504
assert_transient 0 429

# Permanent: 4xx (except 429), successful curl with non-retry HTTP
assert_permanent 0 400
assert_permanent 0 404
assert_permanent 0 422
assert_permanent 0 200

# Backoff grows and respects cap
OPENPUFFER_INGEST_RETRY_BASE_MS=500 OPENPUFFER_INGEST_RETRY_MAX_MS=8000
b1="$(ingest_large_retry_backoff_sec 1)"
b2="$(ingest_large_retry_backoff_sec 2)"
b3="$(ingest_large_retry_backoff_sec 3)"
[[ "$b1" -eq 1 ]] || fail "backoff attempt1 got ${b1}"
[[ "$b2" -eq 1 ]] || fail "backoff attempt2 got ${b2}"
[[ "$b3" -eq 2 ]] || fail "backoff attempt3 got ${b3}"
b6="$(ingest_large_retry_backoff_sec 6)"
[[ "$b6" -le 8 ]] || fail "backoff cap exceeded: ${b6}"

# Failure JSON sidecar shape
INGEST_FAILURE_RECORDS=()
ingest_large_record_failure 3 "batch-00002.json" 6 503 0 true "upsert exhausted retries"
out="$(ingest_large_failures_json)"
echo "$out" | jq -e '.[0].batch == 3 and .[0].http_code == 503 and .[0].transient == true' >/dev/null \
  || fail "failures json: $out"

if command -v shellcheck >/dev/null 2>&1; then
  shellcheck -x scripts/lib/ingest-large-retry.sh
fi

grep -q 'ingest-large-retry.sh' scripts/ingest-large.sh || fail 'ingest-large.sh must source retry lib'
grep -q 'OPENPUFFER_INGEST_START_BATCH' scripts/ingest-large.sh || fail 'missing START_BATCH resume'
grep -q 'ingest_failures' scripts/ingest-large.sh || fail 'missing ingest_failures in JSON'

./scripts/ingest-large.sh --dry-run >/dev/null

echo "ingest-large-retry tests OK"