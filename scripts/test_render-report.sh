#!/usr/bin/env bash
# Offline checks for scripts/render-report.sh (no live benchmark artifacts).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RENDER="$ROOT/scripts/render-report.sh"
FIXTURES="$ROOT/benchmarks/report/fixtures"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

chmod +x "$RENDER"

# Inject a leaky note into a copy of fixtures to verify redaction.
cp "$FIXTURES/large-aws-l1.json" "$TMP/large-aws-l1.json"
cp "$FIXTURES/tpuf-l1.json" "$TMP/tpuf-l1.json"
jq '.notes = "leaked TURBOPUFFER_API_KEY=tpuf_deadbeef_secret for test"' "$TMP/tpuf-l1.json" >"$TMP/tpuf-leak.json"
mv "$TMP/tpuf-leak.json" "$TMP/tpuf-l1.json"

OUT="$TMP/report.md"
"$RENDER" --dry-run --tier l1 \
  --openpuffer-json "$TMP/large-aws-l1.json" \
  --tpuf-json "$TMP/tpuf-l1.json" \
  --output "$OUT" \
  --date 2099-01-01

grep -q "## Methodology" "$OUT"
grep -q "Cold p50 query" "$OUT"
grep -q "recall@10" "$OUT"
grep -q "420" "$OUT"
grep -q "280" "$OUT"
grep -q "Warm p50 query" "$OUT"
grep -q "18" "$OUT"
grep -q "1.50×" "$OUT" || grep -q "1.5" "$OUT"

if grep -E 'tpuf_[A-Za-z0-9]|tpuf_deadbeef' "$OUT"; then
  echo "FAIL: report leaked API key material" >&2
  exit 1
fi
if ! grep -q '\[REDACTED' "$OUT"; then
  echo "FAIL: expected redaction marker in report" >&2
  exit 1
fi

# Default fixture dry-run (no explicit JSON paths).
OUT2="$TMP/report-fixtures.md"
"$RENDER" --dry-run --tier l1 --output "$OUT2" --date 2099-01-02
grep -q "Executive summary" "$OUT2"

echo "render-report tests OK"