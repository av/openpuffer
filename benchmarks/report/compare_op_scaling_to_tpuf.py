#!/usr/bin/env python3
"""Extrapolate openpuffer MinIO scaling tiers to tpuf 10M reference scale.

Reads benchmarks/results/tpuf-official-reference.json and op-scaling-*.json (cold,
10k/50k/100k tiers), fits L ≈ a·N^b in log–log space, extrapolates p50 at 1M/10M
(128-d), applies √dim heuristic toward 10M×1024, and prints a side-by-side table
vs turbopuffer official cold p50 (874 ms).

Usage:
  python3 benchmarks/report/compare_op_scaling_to_tpuf.py
  ./scripts/compare-op-scaling-to-tpuf.sh
"""

from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "benchmarks" / "results"
TPUF_REF = RESULTS / "tpuf-official-reference.json"
DIM_REF = 1024
DIM_OP = 128
N_REF = 10_000_000
N_1M = 1_000_000
FIT_DOC_COUNTS = {10_000, 50_000, 100_000}


@dataclass(frozen=True)
class FitResult:
    a: float
    b: float
    sigma_log: float
    n_points: int
    points: list[tuple[int, float]]


@dataclass(frozen=True)
class Prediction:
    n_docs: int
    dims: int
    p50_ms: float
    p50_lo_ms: float
    p50_hi_ms: float
    label: str


def load_tpuf_cold_p50() -> int:
    data = json.loads(TPUF_REF.read_text(encoding="utf-8"))
    return int(data["latencies_ms"]["cold"]["p50"])


def load_op_scaling_points() -> list[tuple[int, float]]:
    points: list[tuple[int, float]] = []
    for path in sorted(RESULTS.glob("op-scaling-*.json")):
        if "warm" in path.name or "synthetic128" in path.name:
            continue
        row = json.loads(path.read_text(encoding="utf-8"))
        if row.get("path") != "cold":
            continue
        n = int(row["namespace_docs"])
        if n not in FIT_DOC_COUNTS:
            continue
        points.append((n, float(row["p50_ms"])))
    points.sort(key=lambda t: t[0])
    if len(points) < 2:
        raise SystemExit(
            f"need ≥2 cold op-scaling tiers in {FIT_DOC_COUNTS}; found {points}"
        )
    return points


def fit_power_law(points: list[tuple[int, float]]) -> FitResult:
    xs = [math.log(n) for n, _ in points]
    ys = [math.log(l) for _, l in points]
    n = len(points)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    var_x = sum((x - mean_x) ** 2 for x in xs)
    if var_x <= 0:
        raise SystemExit("cannot fit power law: identical doc counts")
    b = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / var_x
    log_a = mean_y - b * mean_x
    a = math.exp(log_a)
    if n > 2:
        residuals = [y - (log_a + b * x) for x, y in zip(xs, ys)]
        sigma = math.sqrt(sum(r * r for r in residuals) / (n - 2))
    else:
        sigma = 0.25  # conservative fallback with only two points
    return FitResult(a=a, b=b, sigma_log=sigma, n_points=n, points=points)


def predict_p50(fit: FitResult, n_docs: int, sigma_mult: float = 0.0) -> float:
    log_l = math.log(fit.a) + fit.b * math.log(n_docs) + sigma_mult * fit.sigma_log
    return math.exp(log_l)


def dim_scale_heuristic(p50_128_ms: float, dims: int = DIM_OP, ref_dims: int = DIM_REF) -> float:
    """Heuristic: latency scales ~√d for probe/dot work (document uncertainty)."""
    return p50_128_ms * math.sqrt(ref_dims / dims)


def build_predictions(fit: FitResult) -> list[Prediction]:
    out: list[Prediction] = []
    for n_docs, label in (
        (N_1M, "extrap 1M × 128 (MinIO)"),
        (N_REF, "extrap 10M × 128 (MinIO)"),
    ):
        mid = predict_p50(fit, n_docs)
        lo = predict_p50(fit, n_docs, sigma_mult=-2.0)
        hi = predict_p50(fit, n_docs, sigma_mult=2.0)
        out.append(
            Prediction(
                n_docs=n_docs,
                dims=DIM_OP,
                p50_ms=mid,
                p50_lo_ms=lo,
                p50_hi_ms=hi,
                label=label,
            )
        )
    mid_10m = predict_p50(fit, N_REF)
    equiv = dim_scale_heuristic(mid_10m)
    equiv_lo = dim_scale_heuristic(predict_p50(fit, N_REF, sigma_mult=-2.0))
    equiv_hi = dim_scale_heuristic(predict_p50(fit, N_REF, sigma_mult=2.0))
    out.append(
        Prediction(
            n_docs=N_REF,
            dims=DIM_REF,
            p50_ms=equiv,
            p50_lo_ms=equiv_lo,
            p50_hi_ms=equiv_hi,
            label="tpuf-equivalent 10M × 1024 (√dim heuristic)",
        )
    )
    return out


def fmt_ms(ms: float) -> str:
    if ms >= 10_000:
        return f"{ms / 1000:.1f}s ({ms:.0f} ms)"
    return f"{ms:.0f}"


def ballpark_verdict(op_equiv_ms: float, tpuf_ms: int) -> str:
    ratio = op_equiv_ms / tpuf_ms
    if ratio < 0.5:
        return f"extrapolated openpuffer is ~{ratio:.2f}× **faster** than tpuf official (unlikely on MinIO vs GCP — treat as model artifact)"
    if ratio <= 2.0:
        return (
            f"extrapolated openpuffer (~{fmt_ms(op_equiv_ms)}) is within **~2×** of tpuf "
            f"official {tpuf_ms} ms — **same order of magnitude** under heroic assumptions "
            "(MinIO→GCP parity, √dim scaling, power-law extrapolation from 10k–100k only)"
        )
    if ratio <= 100:
        return (
            f"extrapolated openpuffer is **~{ratio:.0f}× slower** than tpuf {tpuf_ms} ms — "
            "**not** in the same absolute ballpark on this MinIO harness (order-of-magnitude gap)"
        )
    return (
        f"extrapolated openpuffer is **~{ratio:.0f}× slower** than tpuf {tpuf_ms} ms — "
        "**not** in the same absolute ballpark on this MinIO harness"
    )


def main() -> int:
    tpuf_p50 = load_tpuf_cold_p50()
    points = load_op_scaling_points()
    fit = fit_power_law(points)
    preds = build_predictions(fit)
    dim_factor = math.sqrt(DIM_REF / DIM_OP)

    print("=== openpuffer scaling → turbopuffer 10M reference ===\n")
    print(f"tpuf official cold p50: {tpuf_p50} ms (10M × 1024, GCP, 8 QPS × 30m)\n")

    print("Measured openpuffer cold p50 (MinIO, release + v3):")
    for n, l in fit.points:
        print(f"  {n:>7} docs × 128-d: {l:.0f} ms")
    print()

    print(
        f"Power-law fit L ≈ {fit.a:.4g} · N^{fit.b:.3f} "
        f"(log-space σ≈{fit.sigma_log:.3f}, n={fit.n_points})"
    )
    print(
        f"95% band heuristic: ±2σ in log-space on extrapolation "
        f"(wide with only {fit.n_points} points — not a production SLO)\n"
    )

    print("| Scale | p50 (ms) | 95% band (ms) | Notes |")
    print("|-------|----------|---------------|-------|")
    for p in preds:
        band = f"{p.p50_lo_ms:.0f} – {p.p50_hi_ms:.0f}"
        print(f"| {p.label} | **{p.p50_ms:.0f}** | {band} | extrapolated |")

    equiv = next(p for p in preds if p.dims == DIM_REF)
    print()
    print("### Side-by-side (cold p50)")
    print("| System | Docs × dims | Environment | p50 (ms) |")
    print("|--------|-------------|-------------|----------|")
    print(f"| turbopuffer (official) | 10M × 1024 | GCP managed | **{tpuf_p50}** |")
    mid_10m_128 = predict_p50(fit, N_REF)
    print(
        f"| openpuffer (extrapolated) | 10M × 128 | MinIO (power-law) | **{mid_10m_128:.0f}** "
        f"({fmt_ms(mid_10m_128)}) |"
    )
    print(
        f"| openpuffer (tpuf-equivalent heuristic) | 10M × 1024 | MinIO + ×{dim_factor:.2f} √dim | "
        f"**{equiv.p50_ms:.0f}** ({fmt_ms(equiv.p50_ms)}) |"
    )
    print()
    print("√dim heuristic: L(10M,1024) ≈ L(10M,128) × √(1024/128) ≈ L × 2.83")
    print("Uncertainty: dim exponent unknown; recall/probe plans differ; MinIO ≠ GCP.\n")
    print("### Are we in the same ballpark vs tpuf 874 ms?")
    print(ballpark_verdict(equiv.p50_ms, tpuf_p50))
    print()
    ratio_128 = mid_10m_128 / tpuf_p50
    print(
        f"Raw 10M×128 extrapolation / tpuf: {ratio_128:.1f}× "
        f"({fmt_ms(mid_10m_128)} vs {tpuf_p50} ms)"
    )
    print(f"Heuristic 10M×1024 / tpuf: {equiv.p50_ms / tpuf_p50:.1f}×")

    # Machine-readable line for scripts
    print()
    print(
        "EXTRAP_JSON="
        + json.dumps(
            {
                "fit": {"a": fit.a, "b": fit.b, "sigma_log": fit.sigma_log},
                "measured_points": [{"n": n, "p50_ms": l} for n, l in fit.points],
                "extrap_10m_128_p50_ms": round(mid_10m_128),
                "extrap_10m_1024_heuristic_p50_ms": round(equiv.p50_ms),
                "tpuf_official_cold_p50_ms": tpuf_p50,
                "ratio_heuristic_vs_tpuf": round(equiv.p50_ms / tpuf_p50, 2),
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())