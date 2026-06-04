#!/usr/bin/env bash
# Offline checks for scripts/fill-comparison-from-report.sh (exemplar report).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FILL="$ROOT/scripts/fill-comparison-from-report.sh"
EXEMPLAR="$ROOT/docs/reports/BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

chmod +x "$FILL"

# Refuse NOT MEASURED without --allow-fixture.
if "$FILL" --dry-run --report "$EXEMPLAR" 2>/dev/null; then
  echo "FAIL: expected refusal for NOT MEASURED exemplar" >&2
  exit 1
fi

OUT="$("$FILL" --dry-run --allow-fixture --report "$EXEMPLAR")"

grep -q 'comparison-l1-rows:start' <<<"$OUT"
grep -q 'Ingest wall time (s) | 110 | 95.2 | 1.16×' <<<"$OUT"
grep -q 'Cold p50 query (ms) | 420 | 280 | 1.50×' <<<"$OUT"
grep -q 'recall@10 (num=20) | 0.920 | 0.940 | 0.98×' <<<"$OUT"
grep -q 'Spot-check overlap@10 (10 queries) | mean intersection@10 = 0.690' <<<"$OUT"
grep -q 'fixture dry-run' <<<"$OUT"
grep -q 'BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md' <<<"$OUT"

# In-place fill on a copy of COMPARISON.md (do not mutate committed doc in tests).
cp "$ROOT/docs/COMPARISON.md" "$TMP/COMPARISON.md"
# Ensure markers exist (committed doc should have them).
grep -q 'comparison-l1-rows:start' "$TMP/COMPARISON.md"

"$FILL" --allow-fixture --report "$EXEMPLAR" --comparison "$TMP/COMPARISON.md"

grep -q 'Cold p50 query (ms) | 420 | 280 | 1.50×' "$TMP/COMPARISON.md"
grep -q 'comparison-l1-rows:end' "$TMP/COMPARISON.md"
grep -q 'BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md' "$TMP/COMPARISON.md"

echo "fill-comparison-from-report tests OK"