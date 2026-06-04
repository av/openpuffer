# shellcheck shell=bash
# Retry/resume helpers for scripts/ingest-large.sh (production S3 upsert path).
# Source only — do not execute directly.

# Return 0 when curl exit / HTTP status indicate a transient upsert failure worth retrying.
ingest_large_is_transient_failure() {
  local curl_exit="${1:-0}"
  local http_code="${2:-0}"
  # curl: connection refused, timeout, SSL, recv failure, empty reply, etc.
  case "$curl_exit" in
    6|7|16|18|28|35|52|56) return 0 ;;
  esac
  case "$http_code" in
    000|429|500|502|503|504) return 0 ;;
  esac
  return 1
}

# Exponential backoff sleep in seconds: base_ms * 2^(attempt-1), capped at max_ms.
ingest_large_retry_backoff_sec() {
  local attempt="${1:-1}"
  local base_ms="${OPENPUFFER_INGEST_RETRY_BASE_MS:-500}"
  local max_ms="${OPENPUFFER_INGEST_RETRY_MAX_MS:-30000}"
  python3 -c "
import math
attempt = max(1, int('$attempt'))
base_ms = max(1, int('$base_ms'))
max_ms = max(base_ms, int('$max_ms'))
delay_ms = min(max_ms, base_ms * (2 ** (attempt - 1)))
print(max(1, int(math.ceil(delay_ms / 1000.0))))
"
}

# POST one upsert batch with retries. Sets INGEST_LAST_UPSERT_HTTP_CODE and INGEST_LAST_UPSERT_CURL_EXIT.
# On success returns 0. On permanent failure returns 1.
ingest_large_upsert_batch_with_retry() {
  local url="$1"
  local file="$2"
  local batch_num="$3"
  local max_attempts="${OPENPUFFER_INGEST_RETRY_MAX:-6}"
  local attempt=0
  local http_code=0
  local curl_exit=0

  while [[ "$attempt" -lt "$max_attempts" ]]; do
    attempt=$((attempt + 1))
    http_code=0
    curl_exit=0
    set +e
    http_code="$(
      curl -sS -o /dev/null -w '%{http_code}' \
        -X POST "$url" \
        -H 'Content-Type: application/json' \
        -d @"$file" 2>"${INGEST_CURL_ERR_FILE:-/dev/stderr}"
    )"
    curl_exit=$?
    set -e
    export INGEST_LAST_UPSERT_HTTP_CODE="$http_code"
    export INGEST_LAST_UPSERT_CURL_EXIT="$curl_exit"

    if [[ "$curl_exit" -eq 0 && "$http_code" =~ ^2[0-9][0-9]$ ]]; then
      return 0
    fi

    if ingest_large_is_transient_failure "$curl_exit" "$http_code"; then
      if [[ "$attempt" -lt "$max_attempts" ]]; then
        local backoff
        backoff="$(ingest_large_retry_backoff_sec "$attempt")"
        echo "  batch ${batch_num}/${INGEST_BATCH_TOTAL:-?} $(basename "$file"): transient failure (curl=${curl_exit} http=${http_code}); retry ${attempt}/${max_attempts} in ${backoff}s" >&2
        sleep "$backoff"
        continue
      fi
    fi
    return 1
  done
  return 1
}

# Append one failure record JSON object to INGEST_FAILURE_RECORDS (bash array).
ingest_large_record_failure() {
  local batch_num="$1"
  local batch_file="$2"
  local attempt="$3"
  local http_code="$4"
  local curl_exit="$5"
  local transient="$6"
  local message="$7"
  INGEST_FAILURE_RECORDS+=("$(jq -cn \
    --argjson batch "$batch_num" \
    --arg file "$batch_file" \
    --argjson attempt "$attempt" \
    --argjson http_code "${http_code:-0}" \
    --argjson curl_exit "${curl_exit:-0}" \
    --argjson transient "$transient" \
    --arg message "$message" \
    '{
      batch: $batch,
      file: $file,
      attempt: $attempt,
      http_code: $http_code,
      curl_exit: $curl_exit,
      transient: $transient,
      message: $message
    }')")
}

ingest_large_failures_json() {
  if [[ ${#INGEST_FAILURE_RECORDS[@]} -eq 0 ]]; then
    echo '[]'
    return 0
  fi
  printf '%s\n' "${INGEST_FAILURE_RECORDS[@]}" | jq -s '.'
}