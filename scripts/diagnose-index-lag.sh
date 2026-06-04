#!/usr/bin/env bash
# Poll openpuffer namespace metadata and print human-readable index lag status.
# Use while ingest-large / bench-large wait for index_cursor == wal_commit_seq.
#
# Usage:
#   ./scripts/diagnose-index-lag.sh                    # watch L1 default namespace
#   ./scripts/diagnose-index-lag.sh --tier l2          # bench-large-500000
#   ./scripts/diagnose-index-lag.sh --namespace my-ns --once
#   OPENPUFFER_INGEST_LISTEN=127.0.0.1:8080 ./scripts/diagnose-index-lag.sh
#
# Environment (optional):
#   OPENPUFFER_DIAG_NAMESPACE, OPENPUFFER_DIAG_LISTEN (alias: OPENPUFFER_INGEST_LISTEN)
#   OPENPUFFER_DIAG_POLL_SEC (default 2), OPENPUFFER_DIAG_TIER (default l1)
#
# See docs/BENCHMARKS.md § Index timeout exceeded (ingest-large).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export LARGE_PREFLIGHT_ROOT="$ROOT"
# shellcheck source=scripts/lib/large-benchmark-preflight.sh
source "$ROOT/scripts/lib/large-benchmark-preflight.sh"

TIER="${OPENPUFFER_DIAG_TIER:-${OPENPUFFER_INGEST_TIER:-l1}}"
NAMESPACE="${OPENPUFFER_DIAG_NAMESPACE:-}"
LISTEN="${OPENPUFFER_DIAG_LISTEN:-${OPENPUFFER_INGEST_LISTEN:-127.0.0.1:8080}}"
POLL_SEC="${OPENPUFFER_DIAG_POLL_SEC:-2}"
ONCE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier=*) TIER="${1#*=}"; shift ;;
    --tier)
      shift
      TIER="${1:?--tier requires l1|l2|l3}"
      shift
      ;;
    --namespace=*) NAMESPACE="${1#*=}"; shift ;;
    --namespace|-n)
      shift
      NAMESPACE="${1:?--namespace requires a name}"
      shift
      ;;
    --listen=*) LISTEN="${1#*=}"; shift ;;
    --listen)
      shift
      LISTEN="${1:?--listen requires host:port}"
      shift
      ;;
    --poll-sec=*) POLL_SEC="${1#*=}"; shift ;;
    --poll-sec)
      shift
      POLL_SEC="${1:?--poll-sec requires seconds}"
      shift
      ;;
    --once) ONCE=1; shift ;;
    --watch) ONCE=0; shift ;;
    -h|--help)
      sed -n '2,16p' "$0"
      exit 0
      ;;
    *) echo "diagnose-index-lag: unknown argument: $1" >&2; exit 1 ;;
  esac
done

case "$TIER" in
  l1) TIER_DOCS=100000 ;;
  l2) TIER_DOCS=500000 ;;
  l3) TIER_DOCS=1000000 ;;
  *)
    echo "diagnose-index-lag: unknown tier ${TIER} (use l1, l2, or l3)" >&2
    exit 1
    ;;
esac

if [[ -z "$NAMESPACE" ]]; then
  NAMESPACE="${OPENPUFFER_INGEST_NAMESPACE:-bench-large-${TIER_DOCS}}"
fi

BASE_URL="http://${LISTEN}"
V1_NS_URL="${BASE_URL}/v1/namespaces/${NAMESPACE}"
DEFAULT_INDEX_TIMEOUT="$(large_preflight_tier_index_timeout_sec "$TIER")"

command -v curl >/dev/null 2>&1 || {
  echo "diagnose-index-lag: missing curl" >&2
  exit 1
}
command -v jq >/dev/null 2>&1 || {
  echo "diagnose-index-lag: missing jq" >&2
  exit 1
}

fetch_meta() {
  curl -sf "$V1_NS_URL" 2>/dev/null || echo ""
}

print_header() {
  echo "diagnose-index-lag: namespace=${NAMESPACE} tier=${TIER} listen=${LISTEN}"
  echo "  poll=${POLL_SEC}s tier_index_timeout_default=${DEFAULT_INDEX_TIMEOUT}s"
  echo "  meta: GET ${V1_NS_URL}"
  echo "---"
}

# Prints one status block; returns 0 when indexed (cursor==commit, ann==3, commit>0).
print_status() {
  local meta="$1"
  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  if [[ -z "$meta" ]]; then
    echo "[${ts}] ERROR: cannot reach ${V1_NS_URL} (serve down? wrong namespace?)"
    echo "  hint: start serve or check OPENPUFFER_INGEST_LISTEN / OPENPUFFER_DIAG_LISTEN"
    return 1
  fi

  local cursor commit pref_ann index_status unindexed approx_rows lag ready ann_ok
  cursor="$(echo "$meta" | jq -r '.index_cursor // 0')"
  commit="$(echo "$meta" | jq -r '.wal_commit_seq // 0')"
  pref_ann="$(echo "$meta" | jq -r '.preferred_ann_version // 0')"
  index_status="$(echo "$meta" | jq -r '.index_status // "unknown"')"
  unindexed="$(echo "$meta" | jq -r '.unindexed_bytes // 0')"
  approx_rows="$(echo "$meta" | jq -r '.approx_row_count // 0')"

  lag=0
  if [[ "$commit" =~ ^[0-9]+$ && "$cursor" =~ ^[0-9]+$ ]]; then
    if [[ "$commit" -gt "$cursor" ]]; then
      lag=$((commit - cursor))
    fi
  fi

  ready=false
  ann_ok=false
  if [[ "$commit" != "0" && "$cursor" == "$commit" && "$pref_ann" == "3" ]]; then
    ready=true
  fi
  [[ "$pref_ann" == "3" ]] && ann_ok=true

  echo "[${ts}] index_cursor=${cursor} wal_commit_seq=${commit} lag_segments=${lag}"
  echo "         index_status=${index_status} unindexed_bytes=${unindexed} approx_row_count=${approx_rows}"
  echo "         preferred_ann_version=${pref_ann}"

  if [[ "$ready" == true ]]; then
    echo "         STATUS: INDEXED — safe for bench-large / cold queries"
    return 0
  fi

  if [[ "$commit" == "0" ]]; then
    echo "         STATUS: NO WAL — upsert not started or namespace empty"
    echo "         ACTION: run ./scripts/ingest-large.sh --tier ${TIER} (or resume OPENPUFFER_INGEST_START_BATCH)"
    return 1
  fi

  if [[ "$lag" -gt 0 ]]; then
    local pct=0
    if [[ "$commit" -gt 0 ]]; then
      pct=$(( (cursor * 100) / commit ))
    fi
    echo "         STATUS: CATCHING UP — indexer behind WAL (${pct}% of commits indexed)"
    echo "         ACTION: keep serve running; watch with --watch; if ingest-large timed out:"
    echo "           OPENPUFFER_INGEST_SKIP_UPSERT=1 OPENPUFFER_INGEST_INDEX_TIMEOUT_SEC=${DEFAULT_INDEX_TIMEOUT} \\"
    echo "             ./scripts/ingest-large.sh --tier ${TIER}"
    echo "         TIMEOUT: raise OPENPUFFER_INGEST_INDEX_TIMEOUT_SEC (tier default ${DEFAULT_INDEX_TIMEOUT}s)"
  elif [[ "$ann_ok" != true ]]; then
    echo "         STATUS: WAL CAUGHT UP but preferred_ann_version != 3"
    echo "         ACTION: export OPENPUFFER_ANN_VERSION=3; restart serve; may need delete + re-ingest"
  else
    echo "         STATUS: WAITING (unexpected meta shape)"
  fi
  return 1
}

print_header

if [[ "$ONCE" == "1" ]]; then
  print_status "$(fetch_meta)"
  exit $?
fi

prev_lag=-1
while true; do
  meta="$(fetch_meta)"
  if print_status "$meta"; then
    exit 0
  fi
  if [[ -n "$meta" ]]; then
    lag_now="$(echo "$meta" | jq -r '(.wal_commit_seq // 0) - (.index_cursor // 0)')"
    if [[ "$lag_now" =~ ^[0-9]+$ && "$prev_lag" -ge 0 && "$lag_now" -lt "$prev_lag" ]]; then
      echo "         NOTE: lag decreased (${prev_lag} -> ${lag_now} segments); indexer progressing"
    fi
    prev_lag="$lag_now"
  fi
  sleep "$POLL_SEC"
done