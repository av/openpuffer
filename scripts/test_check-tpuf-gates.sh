#!/usr/bin/env bash
# Offline tests for scripts/check-tpuf-gates.sh (fixture pass + synthetic failures).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CHECK="$ROOT/scripts/check-tpuf-gates.sh"
FIXTURE="$ROOT/benchmarks/report/fixtures/tpuf-l1.json"
chmod +x "$CHECK"
export TURBOPUFFER_BENCH_ENFORCE_GATES=1

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

[[ -f "$FIXTURE" ]] || fail "missing fixture: $FIXTURE"

echo "==> fixture passes tpuf gates"
"$CHECK" "$FIXTURE"

echo "==> enforce=0 always passes on broken JSON"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
jq '.recall_at_10 = 0.1 | .index_up_to_date = false | .cold_query_runs = 1' \
  "$FIXTURE" >"$tmpdir/bad.json"
TURBOPUFFER_BENCH_ENFORCE_GATES=0 "$CHECK" "$tmpdir/bad.json" >/dev/null

echo "==> non-turbopuffer environment skipped (exit 0)"
jq '.environment = "minio"' "$FIXTURE" >"$tmpdir/minio.json"
"$CHECK" "$tmpdir/minio.json" >/dev/null

echo "==> reject low recall"
jq '.recall_at_10 = 0.5' "$FIXTURE" >"$tmpdir/recall.json"
if "$CHECK" "$tmpdir/recall.json" >/dev/null 2>&1; then
  fail "expected failure for recall_at_10=0.5"
fi

echo "==> reject index not up to date"
jq '.index_up_to_date = false' "$FIXTURE" >"$tmpdir/index.json"
if "$CHECK" "$tmpdir/index.json" >/dev/null 2>&1; then
  fail "expected failure for index_up_to_date=false"
fi

echo "==> reject wrong cold_query_runs"
jq '.cold_query_runs = 5' "$FIXTURE" >"$tmpdir/runs.json"
if "$CHECK" "$tmpdir/runs.json" >/dev/null 2>&1; then
  fail "expected failure for cold_query_runs=5"
fi

echo "==> reject environment/tpuf_region mismatch"
jq '.tpuf_region = "aws-eu-central-1"' "$FIXTURE" >"$tmpdir/region.json"
if "$CHECK" "$tmpdir/region.json" >/dev/null 2>&1; then
  fail "expected failure for tpuf_region mismatch"
fi

echo "test_check-tpuf-gates: OK"