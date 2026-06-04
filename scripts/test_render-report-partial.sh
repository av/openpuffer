#!/usr/bin/env bash
# Partial-merge checks: one benchmark JSON side missing (--allow-partial).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RENDER="$ROOT/scripts/render-report.sh"
FIXTURES="$ROOT/benchmarks/report/fixtures"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

chmod +x "$RENDER"

cp "$FIXTURES/large-aws-l1.json" "$TMP/large-aws-l1.json"
cp "$FIXTURES/tpuf-l1.json" "$TMP/tpuf-l1.json"

# openpuffer only (measured + --allow-partial)
OUT_OP="$TMP/partial-op.md"
"$RENDER" --allow-partial --tier l1 \
  --openpuffer-json "$TMP/large-aws-l1.json" \
  --tpuf-json "$TMP/no-tpuf.json" \
  --output "$OUT_OP" \
  --date 2099-07-01 2>"$TMP/partial-op.err"

grep -q 'PARTIAL REPORT' "$OUT_OP"
grep -q 'missing turbopuffer JSON' "$TMP/partial-op.err"
grep -q 'partial report (openpuffer only)' "$TMP/partial-op.err"
grep -q '420' "$OUT_OP"
if grep -qE '\| 280 \|' "$OUT_OP"; then
  echo "FAIL: unexpected tpuf column value in op-only report" >&2
  exit 1
fi
if grep -q 'Comparison interpretation' "$OUT_OP"; then
  echo "FAIL: interpretation must be omitted in partial merge" >&2
  exit 1
fi
grep -q 'Merge status' "$OUT_OP"
grep -q 'ratio —' "$OUT_OP"
grep -q 'Cold p50 query' "$OUT_OP"
grep -q '420' "$OUT_OP"
grep -q 'schema OK' "$TMP/partial-op.err"
grep -q 'single side' "$TMP/partial-op.err"

# turbopuffer only (measured + allow-partial)
OUT_TPUF="$TMP/partial-tpuf.md"
"$RENDER" --allow-partial --tier l1 \
  --openpuffer-json "$TMP/no-op.json" \
  --tpuf-json "$TMP/tpuf-l1.json" \
  --output "$OUT_TPUF" \
  --date 2099-07-02 2>"$TMP/partial-tpuf.err"

grep -q 'PARTIAL REPORT' "$OUT_TPUF"
grep -q 'missing openpuffer JSON' "$TMP/partial-tpuf.err"
grep -q 'partial report (turbopuffer only)' "$TMP/partial-tpuf.err"
grep -q '280' "$OUT_TPUF"
if grep -qE '\| 420 \|' "$OUT_TPUF"; then
  echo "FAIL: unexpected op column value in tpuf-only report" >&2
  exit 1
fi

# Without --allow-partial: one side must abort
if "$RENDER" --tier l1 \
  --openpuffer-json "$TMP/large-aws-l1.json" \
  --tpuf-json "$TMP/no-tpuf.json" \
  --output "$TMP/should-fail.md" \
  --date 2099-07-03 2>"$TMP/no-partial.err"; then
  echo "FAIL: expected abort without --allow-partial" >&2
  exit 1
fi
grep -q 'aborting' "$TMP/no-partial.err"
grep -q 'missing turbopuffer JSON' "$TMP/no-partial.err"

# Both missing for tier must abort even with --allow-partial
if "$RENDER" --allow-partial --tier l1 \
  --openpuffer-json "$TMP/no-op.json" \
  --tpuf-json "$TMP/no-tpuf.json" \
  --output "$TMP/should-fail2.md" \
  --date 2099-07-04 2>"$TMP/both-missing.err"; then
  echo "FAIL: expected abort when both JSON missing" >&2
  exit 1
fi
grep -q 'no tier had any JSON' "$TMP/both-missing.err"

echo "render-report partial tests OK"