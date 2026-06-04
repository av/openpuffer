#!/usr/bin/env python3
"""Extrapolate openpuffer MinIO scaling tiers to tpuf 10M reference scale.

Reads benchmarks/results/tpuf-official-reference.json and op-scaling-*.json (cold),
fits multiple models (power-law, linear, log-linear), validates with R² and
leave-one-out (2-point fit → predict held-out tier), extrapolates p50 at 1M/10M,
applies √dim and linear-dim heuristics toward 10M×1024, back-solves N and ms/doc
for tpuf cold p50, and prints a markdown appendix snippet.

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
from typing import Callable

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "benchmarks" / "results"
TPUF_REF = RESULTS / "tpuf-official-reference.json"
DIM_REF = 1024
DIM_OP = 128
N_REF = 10_000_000
N_1M = 1_000_000
FIT_DOC_COUNTS = {10_000, 50_000, 100_000}
SYNTH128_PATH = RESULTS / "op-scaling-10k-synthetic128.json"


@dataclass(frozen=True)
class MeasuredPoint:
    n: int
    p50_ms: float
    label: str


@dataclass(frozen=True)
class ModelFit:
    name: str
    formula: str
    predict: Callable[[int], float]
    r2: float
    rmse_ms: float
    params: dict[str, float]


@dataclass(frozen=True)
class LooResult:
    held_out_label: str
    held_out_n: int
    actual_ms: float
    predicted_ms: float
    pct_error: float
    train_labels: tuple[str, ...]


def load_tpuf_cold_p50() -> int:
    data = json.loads(TPUF_REF.read_text(encoding="utf-8"))
    return int(data["latencies_ms"]["cold"]["p50"])


def _read_cold_json(path: Path) -> MeasuredPoint | None:
    if not path.is_file():
        return None
    row = json.loads(path.read_text(encoding="utf-8"))
    if row.get("path") != "cold":
        return None
    n = int(row["namespace_docs"])
    label = path.stem.removeprefix("op-scaling-")
    return MeasuredPoint(n=n, p50_ms=float(row["p50_ms"]), label=label)


def load_op_scaling_points() -> list[MeasuredPoint]:
    """Four fit points: 10k stress, 10k synthetic-128, 50k, 100k."""
    points: list[MeasuredPoint] = []
    for path in sorted(RESULTS.glob("op-scaling-*.json")):
        if "warm" in path.name:
            continue
        if path.name == SYNTH128_PATH.name:
            continue
        mp = _read_cold_json(path)
        if mp is None or mp.n not in FIT_DOC_COUNTS:
            continue
        points.append(mp)
    synth = _read_cold_json(SYNTH128_PATH)
    if synth is not None:
        points.append(synth)
    points.sort(key=lambda p: (p.n, p.label))
    if len(points) < 3:
        raise SystemExit(
            f"need ≥3 cold op-scaling tiers (incl. synthetic128); found {points}"
        )
    return points


def collapse_by_n(points: list[MeasuredPoint]) -> list[tuple[int, float]]:
    buckets: dict[int, list[float]] = {}
    for p in points:
        buckets.setdefault(p.n, []).append(p.p50_ms)
    return [(n, sum(v) / len(v)) for n, v in sorted(buckets.items())]


def r2_and_rmse(actual: list[float], predicted: list[float]) -> tuple[float, float]:
    if len(actual) < 2:
        return (1.0, 0.0)
    mean_y = sum(actual) / len(actual)
    ss_tot = sum((y - mean_y) ** 2 for y in actual)
    ss_res = sum((y - p) ** 2 for y, p in zip(actual, predicted))
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 1.0
    rmse = math.sqrt(ss_res / len(actual))
    return (r2, rmse)


def fit_power_law(collapsed: list[tuple[int, float]]) -> ModelFit:
    xs = [math.log(n) for n, _ in collapsed]
    ys = [math.log(l) for _, l in collapsed]
    n = len(collapsed)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    var_x = sum((x - mean_x) ** 2 for x in xs)
    if var_x <= 0:
        raise SystemExit("cannot fit power law: identical doc counts")
    b = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / var_x
    log_a = mean_y - b * mean_x
    a = math.exp(log_a)

    def predict(n_docs: int) -> float:
        return a * (n_docs**b)

    actual = [l for _, l in collapsed]
    pred = [predict(n) for n, _ in collapsed]
    r2, rmse = r2_and_rmse(actual, pred)
    return ModelFit(
        name="power_law",
        formula=f"L ≈ {a:.4g} · N^{b:.3f}",
        predict=predict,
        r2=r2,
        rmse_ms=rmse,
        params={"a": a, "b": b},
    )


def fit_linear(collapsed: list[tuple[int, float]]) -> ModelFit:
    xs = [float(n) for n, _ in collapsed]
    ys = [l for _, l in collapsed]
    n = len(collapsed)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    var_x = sum((x - mean_x) ** 2 for x in xs)
    if var_x <= 0:
        raise SystemExit("cannot fit linear: identical doc counts")
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / var_x
    intercept = mean_y - slope * mean_x

    def predict(n_docs: int) -> float:
        return intercept + slope * n_docs

    actual = ys
    pred = [predict(int(x)) for x in xs]
    r2, rmse = r2_and_rmse(actual, pred)
    return ModelFit(
        name="linear",
        formula=f"L ≈ {intercept:.2f} + {slope:.6g}·N",
        predict=predict,
        r2=r2,
        rmse_ms=rmse,
        params={"intercept": intercept, "slope": slope},
    )


def fit_log_linear(collapsed: list[tuple[int, float]]) -> ModelFit:
    """L ≈ a + b·log(N) — sublinear in doc count (not power law)."""
    xs = [math.log(n) for n, _ in collapsed]
    ys = [l for _, l in collapsed]
    n = len(collapsed)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    var_x = sum((x - mean_x) ** 2 for x in xs)
    if var_x <= 0:
        raise SystemExit("cannot fit log-linear: identical doc counts")
    b = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / var_x
    a = mean_y - b * mean_x

    def predict(n_docs: int) -> float:
        return a + b * math.log(n_docs)

    actual = ys
    pred = [a + b * math.log(n) for n, _ in collapsed]
    r2, rmse = r2_and_rmse(actual, pred)
    return ModelFit(
        name="log_linear",
        formula=f"L ≈ {a:.2f} + {b:.2f}·log(N)",
        predict=predict,
        r2=r2,
        rmse_ms=rmse,
        params={"a": a, "b": b},
    )


def leave_one_out_2fit_tiers(collapsed: list[tuple[int, float]]) -> list[LooResult]:
    """Fit on 2 collapsed tiers, predict the held-out third (power-law)."""
    results: list[LooResult] = []
    for i, (n_hold, l_hold) in enumerate(collapsed):
        train = [t for j, t in enumerate(collapsed) if j != i]
        model = fit_power_law(train)
        pred = model.predict(n_hold)
        pct_err = 100.0 * (pred - l_hold) / l_hold if l_hold else 0.0
        train_labels = tuple(f"N={n:,}" for n, _ in train)
        results.append(
            LooResult(
                held_out_label=f"N={n_hold:,}",
                held_out_n=n_hold,
                actual_ms=l_hold,
                predicted_ms=pred,
                pct_error=pct_err,
                train_labels=train_labels,
            )
        )
    return results


def leave_one_out_4point(points: list[MeasuredPoint]) -> list[LooResult]:
    """For each held-out label, fit power-law on the other three."""
    results: list[LooResult] = []
    for hold in points:
        train_pts = [p for p in points if p.label != hold.label]
        collapsed = collapse_by_n(train_pts)
        if len(collapsed) < 2:
            continue
        model = fit_power_law(collapsed)
        pred = model.predict(hold.n)
        pct_err = 100.0 * (pred - hold.p50_ms) / hold.p50_ms if hold.p50_ms else 0.0
        results.append(
            LooResult(
                held_out_label=hold.label,
                held_out_n=hold.n,
                actual_ms=hold.p50_ms,
                predicted_ms=pred,
                pct_error=pct_err,
                train_labels=tuple(p.label for p in train_pts),
            )
        )
    return results


def backsolve_n_for_target(fit: ModelFit, target_ms: float) -> float | None:
    if target_ms <= 0:
        return None
    if fit.name == "power_law":
        a, b = fit.params["a"], fit.params["b"]
        if a <= 0 or abs(b) < 1e-9:
            return None
        return (target_ms / a) ** (1.0 / b)
    if fit.name == "linear":
        slope = fit.params["slope"]
        intercept = fit.params["intercept"]
        if abs(slope) < 1e-12:
            return None
        n = (target_ms - intercept) / slope
        return n if n > 0 else None
    if fit.name == "log_linear":
        a, b = fit.params["a"], fit.params["b"]
        if abs(b) < 1e-12:
            return None
        n = math.exp((target_ms - a) / b)
        return n if n > 0 else None
    return None


def dim_scale_sqrt(p50_128_ms: float, dims: int = DIM_OP, ref_dims: int = DIM_REF) -> float:
    return p50_128_ms * math.sqrt(ref_dims / dims)


def dim_scale_linear(p50_128_ms: float, dims: int = DIM_OP, ref_dims: int = DIM_REF) -> float:
    """ANN theory: brute/dot portions often ~O(d); labeled estimate only."""
    return p50_128_ms * (ref_dims / dims)


def pick_best_model(models: list[ModelFit]) -> ModelFit:
    return max(models, key=lambda m: (m.r2, -m.rmse_ms))


def build_extrap_notes(points: list[MeasuredPoint], best: ModelFit) -> list[str]:
    """Human-readable caveats for EXTRAP_JSON (outlier history, stability)."""
    p100 = next((p.p50_ms for p in points if p.n == 100_000 and p.label == "100k"), None)
    notes = [
        "Fit uses committed op-scaling-*.json (MinIO release+v3). "
        f"Best model: {best.name}. Not validated on AWS or 1024-d.",
        "2026-06-05 tier sweep (da45441/9c637d1): cold p50 111/525/813 ms; "
        "log_linear extrap ~2160 ms @ 10M×128 (~2.5× tpuf 874 ms); √dim ~7×.",
    ]
    if p100 is not None and p100 >= 700:
        notes.append(
            "SUPERSEDED: linear-only fit on older 86/400/824 ms tiers extrapolated "
            "~81 s @ 10M×128 (~93× slower)—model choice, not a remeasured 100k outlier."
        )
    notes.extend(
        [
            "SUPERSEDED: anecdotal ~7 s @ 100k or ~70× @ 10M from debug build or "
            "host contention—not in committed op-scaling JSON.",
            "100k stability (2026-06-05, three release runs): p50 813 / 857 / 906 ms "
            "(median 857, σ≈47 ms, ±6% vs median); committed JSON keeps sweep 813 ms.",
        ]
    )
    return notes


def fmt_ms(ms: float) -> str:
    if ms >= 10_000:
        return f"{ms / 1000:.1f}s ({ms:.0f} ms)"
    return f"{ms:.0f}"


def fmt_n(n: float) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return f"{n:.0f}"


def ballpark_verdict(op_equiv_ms: float, tpuf_ms: int) -> str:
    ratio = op_equiv_ms / tpuf_ms
    if ratio < 0.5:
        return (
            f"extrapolated openpuffer is ~{ratio:.2f}× **faster** than tpuf official "
            "(unlikely on MinIO vs GCP — treat as model artifact)"
        )
    if ratio <= 2.0:
        return (
            f"extrapolated openpuffer (~{fmt_ms(op_equiv_ms)}) is within **~2×** of tpuf "
            f"official {tpuf_ms} ms — **same order of magnitude** under heroic assumptions"
        )
    if ratio <= 100:
        return (
            f"extrapolated openpuffer is **~{ratio:.0f}× slower** than tpuf {tpuf_ms} ms — "
            "**not** in the same absolute ballpark on this MinIO harness"
        )
    return (
        f"extrapolated openpuffer is **~{ratio:.0f}× slower** than tpuf {tpuf_ms} ms — "
        "**not** in the same absolute ballpark on this MinIO harness"
    )


def markdown_appendix(
    points: list[MeasuredPoint],
    models: list[ModelFit],
    best: ModelFit,
    loo_tiers: list[LooResult],
    loo_4pt: list[LooResult],
    tpuf_p50: int,
    extrap_10m_128: float,
    extrap_10m_sqrt: float,
    extrap_10m_linear_d: float,
    backsolve: dict[str, float | None],
) -> str:
    lines = [
        "### 4.0a Model validation (auto-generated)",
        "",
        "Fit points (cold p50, MinIO release+v3):",
        "",
        "| Label | N | p50 (ms) |",
        "|-------|---|----------|",
    ]
    for p in points:
        lines.append(f"| {p.label} | {p.n:,} | {p.p50_ms:.0f} |")
    lines.extend(["", "| Model | Formula | R² | RMSE (ms) |", "|-------|---------|-----|-----------|"])
    for m in sorted(models, key=lambda x: -x.r2):
        star = " **best**" if m.name == best.name else ""
        lines.append(
            f"| {m.name}{star} | {m.formula} | {m.r2:.4f} | {m.rmse_ms:.1f} |"
        )
    lines.extend(
        [
            "",
            "Leave-one-out — **2-point fit → predict 3rd tier** (collapsed N, power-law):",
            "",
            "| Held out | N | actual (ms) | predicted (ms) | error % |",
            "|----------|---|-------------|----------------|---------|",
        ]
    )
    for r in loo_tiers:
        lines.append(
            f"| {r.held_out_label} | {r.held_out_n:,} | {r.actual_ms:.0f} | "
            f"{r.predicted_ms:.0f} | {r.pct_error:+.1f}% |"
        )
    lines.extend(
        [
            "",
            "Leave-one-out — **4 labels** (fit on 3 → predict held-out):",
            "",
            "| Held out | N | actual (ms) | predicted (ms) | error % |",
            "|----------|---|-------------|----------------|---------|",
        ]
    )
    for r in loo_4pt:
        lines.append(
            f"| {r.held_out_label} | {r.held_out_n:,} | {r.actual_ms:.0f} | "
            f"{r.predicted_ms:.0f} | {r.pct_error:+.1f}% |"
        )
    lines.extend(
        [
            "",
            f"**Best model by R²:** `{best.name}` — {best.formula}",
            "",
            f"| Extrapolation @ 10M×128 ({best.name}) | {extrap_10m_128:.0f} ms |",
            f"| 10M×1024 (√dim estimate) | {extrap_10m_sqrt:.0f} ms |",
            f"| 10M×1024 (linear-d estimate, ANN theory) | {extrap_10m_linear_d:.0f} ms |",
            f"| tpuf official cold | {tpuf_p50} ms |",
            "",
            "**When would openpuffer match tpuf 874 ms?** (back-solve N, same harness assumptions)",
            "",
            "| Model | N @ 874 ms p50 | Notes |",
            "|-------|----------------|-------|",
        ]
    )
    for name, n_val in backsolve.items():
        if n_val is None or n_val <= 0:
            lines.append(f"| {name} | — | infeasible (model / target) |")
        else:
            lines.append(f"| {name} | **{fmt_n(n_val)}** ({n_val:,.0f}) | extrap only |")
    ms_per_doc_op = extrap_10m_128 / N_REF
    ms_per_doc_tpuf = tpuf_p50 / N_REF
    factor = ms_per_doc_op / ms_per_doc_tpuf
    lines.extend(
        [
            "",
            f"**ms/doc @ 10M (cold p50/N):** openpuffer extrap **{ms_per_doc_op * 1e6:.2f} µs/doc**; "
            f"tpuf official **{ms_per_doc_tpuf * 1e6:.2f} µs/doc**; need **~{factor:.0f}×** lower "
            "per-doc latency (or fewer docs) to match tpuf on this normalization.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    tpuf_p50 = load_tpuf_cold_p50()
    points = load_op_scaling_points()
    collapsed = collapse_by_n(points)

    models = [
        fit_power_law(collapsed),
        fit_linear(collapsed),
        fit_log_linear(collapsed),
    ]
    best = pick_best_model(models)
    loo_tiers = leave_one_out_2fit_tiers(collapsed)
    loo_4pt = leave_one_out_4point(points)

    extrap_1m = best.predict(N_1M)
    extrap_10m_128 = best.predict(N_REF)
    extrap_10m_sqrt = dim_scale_sqrt(extrap_10m_128)
    extrap_10m_linear_d = dim_scale_linear(extrap_10m_128)
    dim_factor_sqrt = math.sqrt(DIM_REF / DIM_OP)
    dim_factor_linear = DIM_REF / DIM_OP

    backsolve = {
        m.name: backsolve_n_for_target(m, float(tpuf_p50)) for m in models
    }
    backsolve_best = backsolve[best.name]

    print("=== openpuffer scaling → turbopuffer 10M reference ===\n")
    print(f"tpuf official cold p50: {tpuf_p50} ms (10M × 1024, GCP, 8 QPS × 30m)\n")

    print("Measured openpuffer cold p50 (MinIO, release + v3):")
    for p in points:
        print(f"  {p.n:>7} docs × 128-d ({p.label}): {p.p50_ms:.0f} ms")
    print(f"\nCollapsed tiers for regression (mean @ duplicate N): {collapsed}\n")

    print("### Model comparison (fit on collapsed tiers)")
    print("| Model | Formula | R² | RMSE (ms) |")
    print("|-------|---------|-----|-----------|")
    for m in sorted(models, key=lambda x: -x.r2):
        mark = " ← best" if m.name == best.name else ""
        print(f"| {m.name}{mark} | {m.formula} | {m.r2:.4f} | {m.rmse_ms:.1f} |")
    print()

    print("### Leave-one-out — 2-point fit → predict 3rd tier (collapsed N)")
    print("| Held out | actual | predicted | error % |")
    print("|----------|--------|-----------|---------|")
    for r in loo_tiers:
        print(
            f"| {r.held_out_label} | {r.actual_ms:.0f} | "
            f"{r.predicted_ms:.0f} | {r.pct_error:+.1f}% |"
        )
    print()
    print("### Leave-one-out — 4 labels (fit 3 → predict held-out)")
    print("| Held out | actual | predicted | error % |")
    print("|----------|--------|-----------|---------|")
    for r in loo_4pt:
        print(
            f"| {r.held_out_label} @ {r.held_out_n:,} | {r.actual_ms:.0f} | "
            f"{r.predicted_ms:.0f} | {r.pct_error:+.1f}% |"
        )
    print()

    print(f"Best model: **{best.name}** — {best.formula}\n")
    print("| Scale | p50 (ms) | Notes |")
    print("|-------|----------|-------|")
    print(f"| extrap 1M × 128 | **{extrap_1m:.0f}** | {best.name} |")
    print(f"| extrap 10M × 128 | **{extrap_10m_128:.0f}** | {best.name} |")
    print(
        f"| 10M × 1024 (√dim heuristic) | **{extrap_10m_sqrt:.0f}** | "
        f"×{dim_factor_sqrt:.2f} on 10M×128 |"
    )
    print(
        f"| 10M × 1024 (linear-d **estimate**) | **{extrap_10m_linear_d:.0f}** | "
        f"×{dim_factor_linear:.0f} brute/O(d); not measured |"
    )
    print()

    print("### Side-by-side (cold p50)")
    print("| System | Docs × dims | Environment | p50 (ms) |")
    print("|--------|-------------|-------------|----------|")
    print(f"| turbopuffer (official) | 10M × 1024 | GCP managed | **{tpuf_p50}** |")
    print(
        f"| openpuffer (extrapolated) | 10M × 128 | MinIO ({best.name}) | "
        f"**{extrap_10m_128:.0f}** ({fmt_ms(extrap_10m_128)}) |"
    )
    print(
        f"| openpuffer (√dim estimate) | 10M × 1024 | MinIO + ×{dim_factor_sqrt:.2f} | "
        f"**{extrap_10m_sqrt:.0f}** ({fmt_ms(extrap_10m_sqrt)}) |"
    )
    print(
        f"| openpuffer (linear-d estimate) | 10M × 1024 | MinIO + ×{dim_factor_linear:.0f} | "
        f"**{extrap_10m_linear_d:.0f}** ({fmt_ms(extrap_10m_linear_d)}) |"
    )
    print()
    print("√dim heuristic: L(10M,1024) ≈ L(10M,128) × √(1024/128)")
    print("Linear-d estimate: L(10M,1024) ≈ L(10M,128) × (1024/128) for brute/dot-dominated work")
    print()

    print("### When would openpuffer match tpuf 874 ms?")
    for m in models:
        n_sol = backsolve[m.name]
        if n_sol and n_sol > 0:
            print(f"  {m.name}: N ≈ {fmt_n(n_sol)} ({n_sol:,.0f} docs) @ 128-d")
        else:
            print(f"  {m.name}: no positive N solution")
    ms_per_doc_op = extrap_10m_128 / N_REF
    ms_per_doc_tpuf = tpuf_p50 / N_REF
    print(
        f"\n  Per-doc @ 10M: openpuffer extrap {ms_per_doc_op * 1e6:.2f} µs/doc vs "
        f"tpuf {ms_per_doc_tpuf * 1e6:.2f} µs/doc → need ~{ms_per_doc_op / ms_per_doc_tpuf:.0f}× improvement"
    )
    print()

    print("### Are we in the same ballpark vs tpuf 874 ms?")
    print(ballpark_verdict(extrap_10m_sqrt, tpuf_p50))
    print()
    print(f"Raw 10M×128 / tpuf: {extrap_10m_128 / tpuf_p50:.1f}×")
    print(f"√dim 10M×1024 / tpuf: {extrap_10m_sqrt / tpuf_p50:.1f}×")
    print(f"Linear-d 10M×1024 / tpuf: {extrap_10m_linear_d / tpuf_p50:.1f}×")

    md = markdown_appendix(
        points,
        models,
        best,
        loo_tiers,
        loo_4pt,
        tpuf_p50,
        extrap_10m_128,
        extrap_10m_sqrt,
        extrap_10m_linear_d,
        backsolve,
    )
    print("\n--- MARKDOWN_APPENDIX ---\n")
    print(md)

    power = next(m for m in models if m.name == "power_law")
    sigma_log = 0.0
    if len(collapsed) > 2:
        xs = [math.log(n) for n, _ in collapsed]
        ys = [math.log(l) for _, l in collapsed]
        log_a = math.log(power.params["a"])
        b = power.params["b"]
        residuals = [y - (log_a + b * x) for x, y in zip(xs, ys)]
        sigma_log = math.sqrt(sum(r * r for r in residuals) / (len(collapsed) - 2))

    extrap_notes = build_extrap_notes(points, best)
    extrap_json = {
        "notes": extrap_notes,
        "fit": {
            "best_model": best.name,
            "power_law": power.params,
            "sigma_log": sigma_log,
        },
        "models": {
            m.name: {"r2": round(m.r2, 4), "rmse_ms": round(m.rmse_ms, 2), **m.params}
            for m in models
        },
        "measured_points": [
            {"n": p.n, "p50_ms": p.p50_ms, "label": p.label} for p in points
        ],
        "extrap_1m_128_p50_ms": round(extrap_1m),
        "extrap_10m_128_p50_ms": round(extrap_10m_128),
        "extrap_10m_1024_heuristic_p50_ms": round(extrap_10m_sqrt),
        "extrap_10m_1024_linear_d_estimate_p50_ms": round(extrap_10m_linear_d),
        "tpuf_official_cold_p50_ms": tpuf_p50,
        "ratio_heuristic_vs_tpuf": round(extrap_10m_sqrt / tpuf_p50, 2),
        "ratio_linear_d_vs_tpuf": round(extrap_10m_linear_d / tpuf_p50, 2),
        "backsolve_n_at_tpuf_p50": {
            k: (round(v) if v and v > 0 else None) for k, v in backsolve.items()
        },
        "backsolve_n_best_model": (
            round(backsolve_best) if backsolve_best and backsolve_best > 0 else None
        ),
        "ms_per_doc_us_extrap_10m": round(ms_per_doc_op * 1e6, 3),
        "ms_per_doc_us_tpuf_10m": round(ms_per_doc_tpuf * 1e6, 3),
    }
    print()
    print("EXTRAP_JSON=" + json.dumps(extrap_json))
    return 0


if __name__ == "__main__":
    sys.exit(main())