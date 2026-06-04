#!/usr/bin/env bash
# Smoke gate: compare script succeeds on committed JSON and prints tpuf + extrap lines.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

for f in benchmarks/results/tpuf-official-reference.json \
  benchmarks/results/op-scaling-10k.json \
  benchmarks/results/op-scaling-50k.json \
  benchmarks/results/op-scaling-100k.json; do
  if [[ ! -f "$f" ]]; then
    echo "test_compare-op-scaling-to-tpuf: missing required $f" >&2
    exit 1
  fi
done

out="$(mktemp)"
trap 'rm -f "$out"' EXIT

if ! ./scripts/compare-op-scaling-to-tpuf.sh >"$out" 2>&1; then
  echo "test_compare-op-scaling-to-tpuf: compare script failed" >&2
  cat "$out" >&2
  exit 1
fi

grep -q 'tpuf official cold p50: 874 ms' "$out" \
  || grep -q '\*\*874\*\*' "$out" \
  || { echo "test_compare-op-scaling-to-tpuf: expected tpuf 874 ms in output" >&2; cat "$out" >&2; exit 1; }

grep -q 'extrap 10M × 128' "$out" \
  || { echo "test_compare-op-scaling-to-tpuf: expected extrap 10M × 128 row" >&2; exit 1; }

extrap_line="$(grep '^EXTRAP_JSON=' "$out" || true)"
if [[ -z "$extrap_line" ]]; then
  echo "test_compare-op-scaling-to-tpuf: expected EXTRAP_JSON machine line" >&2
  exit 1
fi

python3 - "$extrap_line" <<'PY'
import json
import sys

payload = sys.argv[1].split("=", 1)[1]
data = json.loads(payload)
tpuf = int(data["tpuf_official_cold_p50_ms"])
extrap_128 = int(data["extrap_10m_128_p50_ms"])
extrap_1024 = int(data["extrap_10m_1024_heuristic_p50_ms"])
if tpuf != 874:
    raise SystemExit(f"tpuf_official_cold_p50_ms={tpuf}, want 874")
# log_linear best-fit on 2026-06-05 tiers → ~2.2s @ 10M×128; prior linear fit → ~80s
if extrap_128 < 1_000 or extrap_128 > 130_000:
    raise SystemExit(f"extrap_10m_128_p50_ms={extrap_128} out of expected range")
if extrap_1024 < 4_000 or extrap_1024 > 450_000:
    raise SystemExit(f"extrap_10m_1024_heuristic_p50_ms={extrap_1024} out of expected range")
if data.get("fit", {}).get("best_model") not in ("linear", "power_law", "log_linear"):
    raise SystemExit("missing or unknown fit.best_model")
if data.get("backsolve_n_best_model", 0) < 50_000 or data.get("backsolve_n_best_model", 0) > 250_000:
    raise SystemExit("backsolve_n_best_model out of expected ~100k range")
if data["ratio_heuristic_vs_tpuf"] < 2:
    raise SystemExit("ratio_heuristic_vs_tpuf too low — expected slower than tpuf official")
models = data.get("models", {})
if not models or "linear" not in models or "power_law" not in models:
    raise SystemExit("expected models.linear and models.power_law in EXTRAP_JSON")
print(
    f"test_compare-op-scaling-to-tpuf: EXTRAP_JSON ok "
    f"(best={data['fit']['best_model']}, 10M×128={extrap_128} ms, heuristic={extrap_1024} ms)"
)
PY

echo "test_compare-op-scaling-to-tpuf: OK"