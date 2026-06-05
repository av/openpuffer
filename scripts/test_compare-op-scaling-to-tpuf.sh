#!/usr/bin/env bash
# Smoke gate: compare script succeeds on committed JSON and prints tpuf + extrap lines.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

for f in benchmarks/results/tpuf-official-reference.json \
  benchmarks/results/op-scaling-10k.json \
  benchmarks/results/op-scaling-50k.json \
  benchmarks/results/op-scaling-100k.json \
  benchmarks/results/op-scaling-10k-warm.json \
  benchmarks/results/op-scaling-100k-warm.json \
  benchmarks/results/scaling-comparison-summary.json; do
  if [[ ! -f "$f" ]]; then
    echo "test_compare-op-scaling-to-tpuf: missing required $f" >&2
    exit 1
  fi
done

# @spec 4xt: 100k×128 cold p50 within 2× of tpuf official 10M×1024 cold p50 (order of magnitude)
python3 - <<'PY'
import json
from pathlib import Path

op = int(json.loads(Path("benchmarks/results/op-scaling-100k.json").read_text())["p50_ms"])
tp = int(
    json.loads(Path("benchmarks/results/tpuf-official-reference.json").read_text())[
        "latencies_ms"
    ]["cold"]["p50"]
)
if tp != 874:
    raise SystemExit(f"tpuf_official cold p50={tp}, want 874")
ratio = max(op, tp) / min(op, tp)
if ratio > 2.0:
    raise SystemExit(
        f"op-scaling 100k cold p50={op} ms vs tpuf official {tp} ms: "
        f"ratio {ratio:.3f} > 2.0 (fact 4xt order-of-magnitude gate)"
    )
print(
    f"test_compare-op-scaling-to-tpuf: 4xt ok "
    f"(100k cold p50={op} ms, tpuf={tp} ms, ratio={ratio:.3f}≤2.0)"
)
PY

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

grep -q 'tpuf official warm p50: 14 ms' "$out" \
  || { echo "test_compare-op-scaling-to-tpuf: expected tpuf warm 14 ms in output" >&2; exit 1; }

grep -q 'Side-by-side (warm p50)' "$out" \
  || { echo "test_compare-op-scaling-to-tpuf: expected warm side-by-side table" >&2; exit 1; }

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
tpuf_warm = int(data.get("tpuf_official_warm_p50_ms", 0))
if tpuf_warm != 14:
    raise SystemExit(f"tpuf_official_warm_p50_ms={tpuf_warm}, want 14")
warm_pts = data.get("warm_measured_points")
if not isinstance(warm_pts, list) or len(warm_pts) < 2:
    raise SystemExit("expected warm_measured_points with 10k and 100k tiers")
ratio_10k = float(data.get("ratio_warm_10k_vs_tpuf", 0))
ratio_100k = float(data.get("ratio_warm_100k_vs_tpuf", 0))
if ratio_10k < 7 or ratio_10k > 9:
    raise SystemExit(f"ratio_warm_10k_vs_tpuf={ratio_10k}, want ~8 (112/14)")
if ratio_100k < 55 or ratio_100k > 65:
    raise SystemExit(f"ratio_warm_100k_vs_tpuf={ratio_100k}, want ~59 (827/14)")
warm_ratios = data.get("warm_ratios_vs_tpuf", {})
if warm_ratios.get("10000") != ratio_10k or warm_ratios.get("100000") != ratio_100k:
    raise SystemExit("warm_ratios_vs_tpuf must match ratio_warm_* fields")
print(
    f"test_compare-op-scaling-to-tpuf: EXTRAP_JSON ok "
    f"(canonical={data['canonical_model']}, 10M×128={extrap_128} ms, "
    f"ratio_vs_tpuf={ratio}, warm_10k={ratio_10k}×, warm_100k={ratio_100k}×, "
    f"heuristic={extrap_1024} ms)"
)
PY

grep -q 'Wrote dashboard summary: benchmarks/results/scaling-comparison-summary.json' "$out" \
  || { echo "test_compare-op-scaling-to-tpuf: expected summary write line" >&2; exit 1; }

python3 - <<'PY'
import json
from pathlib import Path

summary_path = Path("benchmarks/results/scaling-comparison-summary.json")
summary = json.loads(summary_path.read_text())
if summary.get("schema_version") != "scaling_comparison_summary_v1":
    raise SystemExit("bad schema_version in scaling-comparison-summary.json")
tpuf = summary["tpuf_official"]
if tpuf["cold"]["p50_ms"] != 874 or tpuf["warm"]["p50_ms"] != 14:
    raise SystemExit("tpuf_official latencies mismatch")
if summary.get("recommended_extrapolation") != "linear":
    raise SystemExit("recommended_extrapolation must be linear")
extrap_block = summary.get("extrapolations") or {}
linear_10m = int(extrap_block.get("linear_10m_ms", 0))
power_10m = int(extrap_block.get("power_law_10m_ms", 0))
if linear_10m < 80_000 or linear_10m > 95_000:
    raise SystemExit(f"linear_10m_ms {linear_10m} out of range")
if power_10m < 60_000 or power_10m > 75_000:
    raise SystemExit(f"power_law_10m_ms {power_10m} out of range")
fits = summary.get("fits") or {}
for model in ("linear", "power_law"):
    if not {"a", "b", "r2"} <= set(fits.get(model, {})):
        raise SystemExit(f"fits[{model}] missing a/b/r2")
canon = summary["canonical_extrapolation"]
extrap = int(canon["p50_ms"])
if extrap != linear_10m:
    raise SystemExit(f"canonical p50_ms {extrap} != linear_10m_ms {linear_10m}")
if extrap < 80_000 or extrap > 95_000:
    raise SystemExit(f"canonical extrap {extrap} out of range")
if summary["ratios"]["cold_10m_128_vs_tpuf_cold"] != canon["ratio_vs_tpuf_cold"]:
    raise SystemExit("ratio mismatch in summary")
ingest = summary["ratios"].get("ingest_docs_per_sec") or {}
for tier, expect in (("10000", 909.09), ("50000", 3571.43), ("100000", 757.58)):
    got = ingest.get(tier)
    if got is None or abs(got - expect) / expect > 0.02:
        raise SystemExit(f"ingest_docs_per_sec[{tier}]={got} expected ~{expect}")
if summary["confidence"] != "low":
    raise SystemExit("confidence must be low")
if len(summary.get("openpuffer_measured", [])) < 5:
    raise SystemExit("expected ≥5 openpuffer measured tiers")
if "874" not in summary.get("verdict_text", ""):
    raise SystemExit("verdict_text missing tpuf reference")
print(
    "test_compare-op-scaling-to-tpuf: scaling-comparison-summary.json ok "
    f"(extrap={extrap} ms, tiers={len(summary['openpuffer_measured'])})"
)
PY

if ! python3 benchmarks/report/compare_op_scaling_to_tpuf.py --csv >/dev/null; then
  echo "test_compare-op-scaling-to-tpuf: --csv failed" >&2
  exit 1
fi

python3 - <<'PY'
import csv
from pathlib import Path

csv_path = Path("benchmarks/results/scaling-comparison.csv")
with csv_path.open(encoding="utf-8", newline="") as fh:
    rows = list(csv.DictReader(fh))
if len(rows) != 9:
    raise SystemExit(f"expected 9 CSV rows, got {len(rows)}")
extrap = [r for r in rows if r["extrapolated"] == "true"]
if len(extrap) != 1 or extrap[0]["tier"] != "10m-extrap":
    raise SystemExit("expected single 10m-extrap row with extrapolated=true")
tpuf = [r for r in rows if r["system"] == "turbopuffer"]
if len(tpuf) != 2 or {r["cache"] for r in tpuf} != {"cold", "warm"}:
    raise SystemExit("expected turbopuffer cold+warm official rows")
measured = [r for r in rows if r["system"] == "openpuffer" and r["extrapolated"] == "false"]
if len(measured) != 6:
    raise SystemExit(f"expected 6 measured openpuffer rows, got {len(measured)}")
p50 = int(extrap[0]["p50"])
if p50 < 80_000 or p50 > 95_000:
    raise SystemExit(f"extrap p50={p50} out of range")
print(f"test_compare-op-scaling-to-tpuf: scaling-comparison.csv ok ({len(rows)} rows)")
PY

echo "test_compare-op-scaling-to-tpuf: OK"