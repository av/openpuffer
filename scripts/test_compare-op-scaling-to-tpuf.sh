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

grep -q 'Canonical extrapolation model: \*\*linear\*\*' "$out" \
  || { echo "test_compare-op-scaling-to-tpuf: expected canonical linear model line" >&2; exit 1; }

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
if data.get("canonical_model") != "linear":
    raise SystemExit(f"canonical_model={data.get('canonical_model')!r}, want linear")
if int(data.get("extrap_p50_10m_128_ms", 0)) != extrap_128:
    raise SystemExit("extrap_p50_10m_128_ms must match extrap_10m_128_p50_ms")
ratio = float(data.get("ratio_vs_tpuf", 0))
if ratio < 95 or ratio > 105:
    raise SystemExit(f"ratio_vs_tpuf={ratio}, want ~100 on 96/412/880 tiers")
if data.get("confidence") != "low":
    raise SystemExit(f"confidence={data.get('confidence')!r}, want low")
# canonical linear on 96/412/880 → ~87s @ 10M×128
if extrap_128 < 80_000 or extrap_128 > 95_000:
    raise SystemExit(f"extrap_10m_128_p50_ms={extrap_128} out of expected range")
if extrap_1024 < 200_000 or extrap_1024 > 280_000:
    raise SystemExit(f"extrap_10m_1024_heuristic_p50_ms={extrap_1024} out of expected range")
fit = data.get("fit", {})
if fit.get("canonical_model") != "linear":
    raise SystemExit("fit.canonical_model must be linear")
if fit.get("best_model_by_r2") not in ("linear", "power_law", "log_linear"):
    raise SystemExit("missing or unknown fit.best_model_by_r2")
if data.get("backsolve_n_canonical_model", 0) < 50_000 or data.get(
    "backsolve_n_canonical_model", 0
) > 250_000:
    raise SystemExit("backsolve_n_canonical_model out of expected ~100k range")
if data["ratio_heuristic_vs_tpuf"] < 200:
    raise SystemExit("ratio_heuristic_vs_tpuf too low — expected slower than tpuf official")
models = data.get("models", {})
if not models or "linear" not in models or "power_law" not in models:
    raise SystemExit("expected models.linear and models.power_law in EXTRAP_JSON")
notes = data.get("notes")
if not isinstance(notes, list) or len(notes) < 2:
    raise SystemExit("expected notes[] with stability/outlier caveats in EXTRAP_JSON")
if not any("SUPERSEDED" in str(n) for n in notes):
    raise SystemExit("expected SUPERSEDED outlier note in EXTRAP_JSON notes")
print(
    f"test_compare-op-scaling-to-tpuf: EXTRAP_JSON ok "
    f"(canonical={data['canonical_model']}, 10M×128={extrap_128} ms, "
    f"ratio_vs_tpuf={ratio}, heuristic={extrap_1024} ms)"
)
PY

echo "test_compare-op-scaling-to-tpuf: OK"