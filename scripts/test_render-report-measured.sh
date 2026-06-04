#!/usr/bin/env bash
# Measured-mode checks for scripts/render-report.sh (fixture merge simulating live G5).
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

OUT="$TMP/measured-report.md"
"$RENDER" --tier l1 \
  --openpuffer-json "$TMP/large-aws-l1.json" \
  --tpuf-json "$TMP/tpuf-l1.json" \
  --output "$OUT" \
  --date 2099-06-01

grep -q "## Comparison interpretation (tier l1)" "$OUT"
grep -q "within ~2×" "$OUT"
grep -q "1.50× slower" "$OUT"
grep -q "Recall@10:" "$OUT"
grep -qE 'within ±0.02|Warning:' "$OUT"
grep -q "### Redacted JSON snapshot @ tier l1" "$OUT"
grep -q 'cold_large_l1' "$OUT"
grep -q 'cold_tpuf_l1' "$OUT"

if grep -E 'tpuf_deadbeef|TURBOPUFFER_API_KEY=tpuf_' "$OUT"; then
  echo "FAIL: measured report leaked API key material" >&2
  exit 1
fi
if grep -q 'tpuf_driver' "$OUT"; then
  :
else
  echo "FAIL: methodology should still reference tpuf_driver path" >&2
  exit 1
fi

# Secret scan blocks measured merge when JSON still contains keys.
LEAKY="$TMP/tpuf-leaky.json"
jq '.notes = "leaked TURBOPUFFER_API_KEY=tpuf_deadbeef_secret"' \
  "$FIXTURES/tpuf-l1.json" >"$LEAKY"
if "$RENDER" --tier l1 \
  --openpuffer-json "$TMP/large-aws-l1.json" \
  --tpuf-json "$LEAKY" \
  --output "$TMP/leak-fail.md" \
  --date 2099-06-05 2>"$TMP/leak.err"; then
  echo "FAIL: expected secret scan failure" >&2
  exit 1
fi
grep -q 'possible secret' "$TMP/leak.err"

# Schema validation: missing required field must abort measured merge.
BAD="$TMP/bad-op.json"
jq 'del(.recall_at_10)' "$TMP/large-aws-l1.json" >"$BAD"
if "$RENDER" --tier l1 \
  --openpuffer-json "$BAD" \
  --tpuf-json "$TMP/tpuf-l1.json" \
  --output "$TMP/should-fail.md" \
  --date 2099-06-02 2>"$TMP/validate.err"; then
  echo "FAIL: expected schema validation failure" >&2
  exit 1
fi
grep -q 'schema missing' "$TMP/validate.err"

# Workload mismatch must abort.
MISMATCH="$TMP/mismatch-tpuf.json"
jq '.namespace_docs = 99999' "$TMP/tpuf-l1.json" >"$MISMATCH"
if "$RENDER" --tier l1 \
  --openpuffer-json "$TMP/large-aws-l1.json" \
  --tpuf-json "$MISMATCH" \
  --output "$TMP/should-fail2.md" \
  --date 2099-06-03 2>"$TMP/mismatch.err"; then
  echo "FAIL: expected workload mismatch failure" >&2
  exit 1
fi
grep -q 'workload mismatch' "$TMP/mismatch.err"

# Missing file must abort before merge.
if "$RENDER" --tier l1 \
  --openpuffer-json "$TMP/no-such-op.json" \
  --tpuf-json "$TMP/tpuf-l1.json" \
  --output "$TMP/should-fail3.md" \
  --date 2099-06-04 2>"$TMP/missing.err"; then
  echo "FAIL: expected missing JSON failure" >&2
  exit 1
fi
grep -q 'missing openpuffer JSON' "$TMP/missing.err"

echo "render-report measured-mode tests OK"