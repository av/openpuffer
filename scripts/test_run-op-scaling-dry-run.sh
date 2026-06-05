#!/usr/bin/env bash
# Smoke: --dry-run plans tiers / compare paths without cargo or summary writes.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

out="$(mktemp)"
trap 'rm -f "$out"' EXIT

if ./scripts/run-op-scaling-benchmark.sh --dry-run >"$out" 2>&1; then
  :
else
  echo "test_run-op-scaling-dry-run: run-op-scaling --dry-run failed" >&2
  cat "$out" >&2
  exit 1
fi

grep -q 'run-op-scaling dry-run OK' "$out" \
  || { echo "test_run-op-scaling-dry-run: missing dry-run banner" >&2; cat "$out" >&2; exit 1; }
grep -q 'no cargo/docker' "$out" \
  || { echo "test_run-op-scaling-dry-run: expected no cargo/docker note" >&2; exit 1; }
grep -q 'op-scaling-10k.json' "$out" \
  || { echo "test_run-op-scaling-dry-run: expected 10k output path" >&2; exit 1; }
grep -q 'CHANGELOG ingest' "$out" \
  || { echo "test_run-op-scaling-dry-run: expected CHANGELOG wall-time hint" >&2; exit 1; }
if grep -qE 'run-op-scaling: (building|10k cold|50k cold|100k cold)' "$out"; then
  echo "test_run-op-scaling-dry-run: dry-run must not execute bench tiers" >&2
  exit 1
fi

sub="$(mktemp)"
trap 'rm -f "$out" "$sub"' EXIT
./scripts/run-op-scaling-benchmark.sh --dry-run 10k 100k >"$sub" 2>&1
grep -q 'Planned tiers (2):' "$sub" \
  || { echo "test_run-op-scaling-dry-run: expected 2 planned tiers" >&2; cat "$sub" >&2; exit 1; }
grep -q 'op-scaling-100k.json' "$sub" \
  || { echo "test_run-op-scaling-dry-run: expected 100k in subset" >&2; exit 1; }
if grep -q 'op-scaling-50k.json' "$sub"; then
  echo "test_run-op-scaling-dry-run: subset must not include 50k" >&2
  exit 1
fi

cmp_out="$(mktemp)"
trap 'rm -f "$out" "$sub" "$cmp_out" "$summary_bak"' EXIT
summary_bak=""
if [[ -f benchmarks/results/scaling-comparison-summary.json ]]; then
  summary_bak="$(mktemp)"
  cp benchmarks/results/scaling-comparison-summary.json "$summary_bak"
fi

if ! ./scripts/compare-op-scaling-to-tpuf.sh --dry-run >"$cmp_out" 2>&1; then
  echo "test_run-op-scaling-dry-run: compare --dry-run failed" >&2
  cat "$cmp_out" >&2
  exit 1
fi

grep -q 'compare-op-scaling dry-run OK' "$cmp_out" \
  || { echo "test_run-op-scaling-dry-run: missing compare dry-run banner" >&2; cat "$cmp_out" >&2; exit 1; }
grep -q 'would_read:' "$cmp_out" \
  || { echo "test_run-op-scaling-dry-run: expected would_read paths" >&2; exit 1; }
grep -q 'tpuf-official-reference.json' "$cmp_out" \
  || { echo "test_run-op-scaling-dry-run: expected tpuf ref path" >&2; exit 1; }
grep -q 'summary_ratios:' "$cmp_out" \
  || { echo "test_run-op-scaling-dry-run: expected summary_ratios block" >&2; exit 1; }
grep -q 'cold_10m_128_vs_tpuf_cold' "$cmp_out" \
  || { echo "test_run-op-scaling-dry-run: expected cold extrap ratio key" >&2; exit 1; }

if [[ -n "$summary_bak" ]]; then
  mv -f "$summary_bak" benchmarks/results/scaling-comparison-summary.json
  summary_bak=""
  mtime_before="$(stat -c %Y benchmarks/results/scaling-comparison-summary.json)"
  sleep 1
  ./scripts/compare-op-scaling-to-tpuf.sh --dry-run >/dev/null
  mtime_after="$(stat -c %Y benchmarks/results/scaling-comparison-summary.json)"
  if [[ "$mtime_before" != "$mtime_after" ]]; then
    echo "test_run-op-scaling-dry-run: compare --dry-run must not rewrite summary JSON" >&2
    exit 1
  fi
fi

echo "test_run-op-scaling-dry-run: OK"