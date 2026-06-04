#!/usr/bin/env bash
# Offline tests for scripts/check-large-aws-gates.sh (fixture pass + synthetic failures).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CHECK="$ROOT/scripts/check-large-aws-gates.sh"
FIXTURE="$ROOT/benchmarks/report/fixtures/large-aws-l1.json"
chmod +x "$CHECK"
export OPENPUFFER_BENCH_ENFORCE_GATES=1

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

[[ -f "$FIXTURE" ]] || fail "missing fixture: $FIXTURE"

echo "==> fixture passes AWS gates"
"$CHECK" "$FIXTURE"

echo "==> enforce=0 always passes on broken JSON"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
jq '.storage_roundtrips = 99 | .recall_at_10 = 0.1 | .p50_query_latency_ms = 9000' \
  "$FIXTURE" >"$tmpdir/bad.json"
OPENPUFFER_BENCH_ENFORCE_GATES=0 "$CHECK" "$tmpdir/bad.json" >/dev/null

echo "==> minio environment skipped (exit 0)"
jq '.environment = "minio"' "$FIXTURE" >"$tmpdir/minio.json"
"$CHECK" "$tmpdir/minio.json" >/dev/null

echo "==> reject high storage_roundtrips"
jq '.storage_roundtrips = 5' "$FIXTURE" >"$tmpdir/rt.json"
if "$CHECK" "$tmpdir/rt.json" >/dev/null 2>&1; then
  fail "expected failure for storage_roundtrips=5"
fi

echo "==> reject low recall"
jq '.recall_at_10 = 0.5' "$FIXTURE" >"$tmpdir/recall.json"
if "$CHECK" "$tmpdir/recall.json" >/dev/null 2>&1; then
  fail "expected failure for recall_at_10=0.5"
fi

echo "==> reject high p50"
jq '.p50_query_latency_ms = 600' "$FIXTURE" >"$tmpdir/p50.json"
if "$CHECK" "$tmpdir/p50.json" >/dev/null 2>&1; then
  fail "expected failure for p50_query_latency_ms=600"
fi

echo "==> reject ann v2"
jq '.preferred_ann_version = 2' "$FIXTURE" >"$tmpdir/ann.json"
if "$CHECK" "$tmpdir/ann.json" >/dev/null 2>&1; then
  fail "expected failure for preferred_ann_version=2"
fi

echo "==> reject index not caught up"
jq '.index_cursor_eq_wal_commit_seq = false' "$FIXTURE" >"$tmpdir/index.json"
if "$CHECK" "$tmpdir/index.json" >/dev/null 2>&1; then
  fail "expected failure for index_cursor_eq_wal_commit_seq=false"
fi

echo "test_check-large-aws-gates: OK"