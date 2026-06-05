#!/usr/bin/env python3
"""Extrapolate openpuffer MinIO scaling tiers to tpuf 10M reference scale.

Reads benchmarks/results/tpuf-official-reference.json and op-scaling-*.json (cold),
fits multiple models (power-law, linear, log-linear), validates with R² and
leave-one-out (2-point fit → predict held-out tier), extrapolates p50 at 1M/10M
using a **single canonical model** (default: linear), applies √dim and linear-dim
heuristics toward 10M×1024, back-solves N and ms/doc for tpuf cold p50, and prints
a markdown appendix snippet.

Canonical extrapolation uses **linear** by default (not auto best-by-R²). On
committed tiers 96/412/880 ms, linear has R²≈0.998 vs log_linear≈0.89; prior
log_linear ~2.2 s @ 10M×128 used superseded 111/525/813 tiers. Override:
``--model=power_law|log_linear``.

Usage:
  python3 benchmarks/report/compare_op_scaling_to_tpuf.py
  python3 benchmarks/report/compare_op_scaling_to_tpuf.py --verdict-only
  python3 benchmarks/report/compare_op_scaling_to_tpuf.py --write-summary
  python3 benchmarks/report/compare_op_scaling_to_tpuf.py --csv
  python3 benchmarks/report/compare_op_scaling_to_tpuf.py --model=log_linear
  ./scripts/compare-op-scaling-to-tpuf.sh
  ./scripts/compare-op-scaling-to-tpuf.sh --dry-run
  ./scripts/print-scaling-verdict.sh

``--dry-run``: list committed JSON paths and summary ratios only (no writes).

Writes ``benchmarks/results/scaling-comparison-summary.json`` on every full run
(dashboard artifact; validated by validate-benchmark-json.sh).
"""

from __future__ import annotations

import csv
import json
import math
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "benchmarks" / "results"
TPUF_REF = RESULTS / "tpuf-official-reference.json"
SUMMARY_PATH = RESULTS / "scaling-comparison-summary.json"
CSV_PATH = RESULTS / "scaling-comparison.csv"
SUMMARY_SCHEMA_VERSION = "scaling_comparison_summary_v1"
CSV_COLUMNS = (
    "system",
    "tier",
    "docs",
    "dims",
    "cache",
    "p50",
    "p90",
    "p99",
    "source",
    "extrapolated",
)
DIM_REF = 1024
DIM_OP = 128
N_REF = 10_000_000
N_1M = 1_000_000
FIT_DOC_COUNTS = {10_000, 50_000, 100_000}
SYNTH128_PATH = RESULTS / "op-scaling-10k-synthetic128.json"

# Single canonical extrapolation model for verdict, EXTRAP_JSON, and reports.
# Justification: on committed 96/412/880 ms tiers, linear R²≈0.998 (iteration 6
# multi-model fit); log_linear was best only on superseded 111/525/813 sweep.
CANONICAL_MODEL = "linear"
CANONICAL_MODEL_JUSTIFICATION = (
    "linear doc-count extrapolation (fixed default): best R² on committed "
    "96/412/880 ms tiers; avoids auto-switching to log_linear (~2.5×) when "
    "tiers refresh. turbopuffer official is ONE point @ 10M — extrap uncertainty "
    "dominates any ratio vs 874 ms."
)
VALID_MODELS = frozenset({"linear", "power_law", "log_linear"})


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


def load_tpuf_reference() -> dict:
    return json.loads(TPUF_REF.read_text(encoding="utf-8"))


def load_tpuf_cold_p50() -> int:
    return int(load_tpuf_reference()["latencies_ms"]["cold"]["p50"])


def load_tpuf_warm_p50() -> int:
    return int(load_tpuf_reference()["latencies_ms"]["warm"]["p50"])


def load_tpuf_latency_triple(path_key: str) -> dict[str, int]:
    lat = load_tpuf_reference()["latencies_ms"][path_key]
    return {
        "p50_ms": int(lat["p50"]),
        "p90_ms": int(lat["p90"]),
        "p99_ms": int(lat["p99"]),
    }


def load_tpuf_official_block() -> dict[str, Any]:
    ref = load_tpuf_reference()
    return {
        "namespace_docs": int(ref["workload"]["document_count"]),
        "dimensions": int(ref["workload"]["dimensions"]),
        "cold": load_tpuf_latency_triple("cold"),
        "warm": load_tpuf_latency_triple("warm"),
    }


def load_op_measured_tiers() -> list[dict[str, Any]]:
    """All committed op-scaling-*.json tiers with p50/p90/p99 for dashboards."""
    rows: list[dict[str, Any]] = []
    for path in sorted(RESULTS.glob("op-scaling-*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        row: dict[str, Any] = {
            "artifact": path.name,
            "label": path.stem.removeprefix("op-scaling-"),
            "path": data["path"],
            "namespace_docs": int(data["namespace_docs"]),
            "dimensions": int(data["dimensions"]),
            "p50_ms": int(data["p50_ms"]),
            "p90_ms": int(data["p90_ms"]),
            "p99_ms": int(data["p99_ms"]),
        }
        if data.get("ingest_wall_secs") is not None:
            row["ingest_wall_secs"] = float(data["ingest_wall_secs"])
        if data.get("docs_per_sec") is not None:
            row["docs_per_sec"] = float(data["docs_per_sec"])
        rows.append(row)
    rows.sort(key=lambda r: (r["path"], r["namespace_docs"], r["label"]))
    return rows


def resolve_git_commit() -> str:
    try:
        out = subprocess.run(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return out.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        pass
    commits = [
        json.loads(p.read_text(encoding="utf-8")).get("git_commit", "")
        for p in sorted(RESULTS.glob("op-scaling-*.json"))
    ]
    commits = [c for c in commits if c]
    return commits[-1] if commits else "unknown"


def utc_now_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_scaling_comparison_summary(
    model_override: str | None = None,
) -> dict[str, Any]:
    """Single JSON artifact for dashboards (schema: scaling-comparison-summary)."""
    snap = compute_comparison(model_override)
    tpuf_p50 = snap.tpuf_p50
    collapsed = collapse_by_n(load_op_scaling_points())
    canonical = resolve_canonical_model(
        [
            fit_power_law(collapsed),
            fit_linear(collapsed),
            fit_log_linear(collapsed),
        ],
        model_override,
    )
    extrap_10m_sqrt = round(snap.extrap_10m_sqrt)
    p100 = next((ms for n, ms in snap.measured_tiers if n == 100_000), None)
    warm_10k = snap.warm_ratios_vs_tpuf.get(10_000)
    warm_100k = snap.warm_ratios_vs_tpuf.get(100_000)
    ratios: dict[str, Any] = {
        "cold_10m_128_vs_tpuf_cold": round(snap.ratio_vs_tpuf, 2),
        "heuristic_10m_1024_vs_tpuf_cold": round(extrap_10m_sqrt / tpuf_p50, 2),
    }
    if p100 is not None:
        ratios["cold_100k_vs_tpuf_cold"] = round(p100 / tpuf_p50, 3)
    if warm_10k is not None:
        ratios["warm_10k_vs_tpuf_warm"] = round(warm_10k, 2)
    if warm_100k is not None:
        ratios["warm_100k_vs_tpuf_warm"] = round(warm_100k, 2)
    ingest_rows = load_op_ingest_throughput()
    if ingest_rows:
        ratios["ingest_docs_per_sec"] = {
            str(n): round(dps, 2) for n, _wall, dps in ingest_rows
        }
    backsolve_n = backsolve_n_for_target(canonical, float(tpuf_p50))
    fits, extrapolations, recommended = build_fit_extrapolation_block(collapsed)
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "timestamp_utc": utc_now_timestamp(),
        "git_commit": resolve_git_commit(),
        "tpuf_official": load_tpuf_official_block(),
        "openpuffer_measured": load_op_measured_tiers(),
        "fits": fits,
        "extrapolations": extrapolations,
        "recommended_extrapolation": recommended,
        "canonical_extrapolation": {
            "model": snap.canonical_model,
            "namespace_docs": N_REF,
            "dimensions": DIM_OP,
            "p50_ms": round(snap.extrap_10m_128),
            "p50_10m_1024_sqrt_dim_ms": extrap_10m_sqrt,
            "ratio_vs_tpuf_cold": round(snap.ratio_vs_tpuf, 2),
            "backsolve_n_at_tpuf_p50": (
                round(backsolve_n) if backsolve_n and backsolve_n > 0 else None
            ),
        },
        "ratios": ratios,
        "confidence": snap.confidence,
        "verdict_text": operator_verdict_paragraph(snap),
    }


def write_scaling_comparison_summary(
    model_override: str | None = None,
    path: Path = SUMMARY_PATH,
) -> Path:
    summary = build_scaling_comparison_summary(model_override)
    path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return path


def _csv_row(
    *,
    system: str,
    tier: str,
    docs: int,
    dims: int,
    cache: str,
    p50: int | float | None,
    p90: int | float | None,
    p99: int | float | None,
    source: str,
    extrapolated: bool,
) -> dict[str, str]:
    def cell(v: int | float | None) -> str:
        if v is None:
            return ""
        if isinstance(v, float) and v == int(v):
            return str(int(v))
        return str(int(v)) if isinstance(v, float) else str(v)

    return {
        "system": system,
        "tier": tier,
        "docs": str(docs),
        "dims": str(dims),
        "cache": cache,
        "p50": cell(p50),
        "p90": cell(p90),
        "p99": cell(p99),
        "source": source,
        "extrapolated": "true" if extrapolated else "false",
    }


def build_scaling_comparison_csv_rows(
    model_override: str | None = None,
) -> list[dict[str, str]]:
    """Rows for spreadsheets: tpuf official, openpuffer measured, canonical extrap."""
    rows: list[dict[str, str]] = []
    ref = load_tpuf_reference()
    tpuf_docs = int(ref["workload"]["document_count"])
    tpuf_dims = int(ref["workload"]["dimensions"])
    tpuf_source = TPUF_REF.name
    for cache in ("cold", "warm"):
        lat = load_tpuf_latency_triple(cache)
        rows.append(
            _csv_row(
                system="turbopuffer",
                tier="10m",
                docs=tpuf_docs,
                dims=tpuf_dims,
                cache=cache,
                p50=lat["p50_ms"],
                p90=lat["p90_ms"],
                p99=lat["p99_ms"],
                source=tpuf_source,
                extrapolated=False,
            )
        )
    for measured in load_op_measured_tiers():
        rows.append(
            _csv_row(
                system="openpuffer",
                tier=str(measured["label"]),
                docs=int(measured["namespace_docs"]),
                dims=int(measured["dimensions"]),
                cache=str(measured["path"]),
                p50=int(measured["p50_ms"]),
                p90=int(measured["p90_ms"]),
                p99=int(measured["p99_ms"]),
                source=str(measured["artifact"]),
                extrapolated=False,
            )
        )
    snap = compute_comparison(model_override)
    extrap_p50 = round(snap.extrap_10m_128)
    rows.append(
        _csv_row(
            system="openpuffer",
            tier="10m-extrap",
            docs=N_REF,
            dims=DIM_OP,
            cache="cold",
            p50=extrap_p50,
            p90=None,
            p99=None,
            source=f"compare_op_scaling_to_tpuf.py:{snap.canonical_model}@10M",
            extrapolated=True,
        )
    )
    return rows


def write_scaling_comparison_csv(
    model_override: str | None = None,
    path: Path = CSV_PATH,
) -> Path:
    rows = build_scaling_comparison_csv_rows(model_override)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def load_tpuf_write_commit_ms_claim() -> int:
    ref = load_tpuf_reference()
    write_path = ref.get("write_path") or {}
    return int(write_path.get("durable_commit_latency_ms_claim", 200))


def load_op_ingest_throughput() -> list[tuple[int, float, float]]:
    """(namespace_docs, ingest_wall_secs, docs_per_sec) for cold tiers with ingest fields."""
    rows: list[tuple[int, float, float]] = []
    for path in sorted(RESULTS.glob("op-scaling-*.json")):
        if "warm" in path.name or path.name == SYNTH128_PATH.name:
            continue
        row = json.loads(path.read_text(encoding="utf-8"))
        if row.get("path") != "cold":
            continue
        wall = row.get("ingest_wall_secs")
        dps = row.get("docs_per_sec")
        if wall is None or dps is None:
            continue
        rows.append((int(row["namespace_docs"]), float(wall), float(dps)))
    rows.sort(key=lambda t: t[0])
    return rows


def _read_cold_json(path: Path) -> MeasuredPoint | None:
    if not path.is_file():
        return None
    row = json.loads(path.read_text(encoding="utf-8"))
    if row.get("path") != "cold":
        return None
    n = int(row["namespace_docs"])
    label = path.stem.removeprefix("op-scaling-")
    return MeasuredPoint(n=n, p50_ms=float(row["p50_ms"]), label=label)


def _read_warm_json(path: Path) -> MeasuredPoint | None:
    if not path.is_file():
        return None
    row = json.loads(path.read_text(encoding="utf-8"))
    if row.get("path") != "warm":
        return None
    n = int(row["namespace_docs"])
    label = path.stem.removeprefix("op-scaling-")
    return MeasuredPoint(n=n, p50_ms=float(row["p50_ms"]), label=label)


def load_op_warm_points() -> list[MeasuredPoint]:
    """Warm tiers from op-scaling-*-warm.json (10k, 100k)."""
    points: list[MeasuredPoint] = []
    for path in sorted(RESULTS.glob("op-scaling-*-warm.json")):
        mp = _read_warm_json(path)
        if mp is not None:
            points.append(mp)
    points.sort(key=lambda p: p.n)
    return points


def warm_ratios_vs_tpuf(
    warm_points: list[MeasuredPoint], tpuf_warm_p50: int
) -> dict[int, float]:
    if tpuf_warm_p50 <= 0:
        return {}
    return {p.n: p.p50_ms / tpuf_warm_p50 for p in warm_points}


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
    """Highest R² on collapsed tiers (diagnostic only; not used for canonical extrap)."""
    return max(models, key=lambda m: (m.r2, -m.rmse_ms))


def parse_model_arg(argv: list[str]) -> str | None:
    for arg in argv:
        if arg.startswith("--model="):
            name = arg.split("=", 1)[1].strip()
            if name not in VALID_MODELS:
                raise SystemExit(
                    f"unknown --model={name!r}; choose from {sorted(VALID_MODELS)}"
                )
            return name
    return None


def resolve_canonical_model(
    models: list[ModelFit], override: str | None = None
) -> ModelFit:
    name = override or CANONICAL_MODEL
    for m in models:
        if m.name == name:
            return m
    raise SystemExit(f"canonical model {name!r} missing from fit set")


def fit_ab_params(fit: ModelFit) -> tuple[float, float]:
    """Serialize fit params as (a, b) for dashboard JSON.

    - power_law: L ≈ a·N^b
    - linear: L ≈ a + b·N  (intercept=a, slope=b)
    """
    if fit.name == "power_law":
        return (fit.params["a"], fit.params["b"])
    if fit.name == "linear":
        return (fit.params["intercept"], fit.params["slope"])
    raise ValueError(f"fit_ab_params: unsupported model {fit.name!r}")


def fit_summary_block(fit: ModelFit) -> dict[str, float]:
    a, b = fit_ab_params(fit)
    return {"a": round(a, 6), "b": round(b, 6), "r2": round(fit.r2, 6)}


def build_fit_extrapolation_block(
    collapsed: list[tuple[int, float]],
) -> tuple[dict[str, dict[str, float]], dict[str, int], str]:
    """fits + extrapolations + recommended_extrapolation for summary JSON."""
    linear = fit_linear(collapsed)
    power = fit_power_law(collapsed)
    fits = {
        "linear": fit_summary_block(linear),
        "power_law": fit_summary_block(power),
    }
    extrapolations = {
        "linear_10m_ms": round(linear.predict(N_REF)),
        "power_law_10m_ms": round(power.predict(N_REF)),
    }
    return fits, extrapolations, CANONICAL_MODEL


def extrap_confidence() -> str:
    """tpuf has one official cold point; 10M openpuffer is unmeasured."""
    return "low"


def build_extrap_notes(
    points: list[MeasuredPoint],
    canonical: ModelFit,
    best: ModelFit,
    extrap_10m_128: float,
    tpuf_p50: int,
) -> list[str]:
    """Human-readable caveats for EXTRAP_JSON (outlier history, stability)."""
    ratio = extrap_10m_128 / tpuf_p50
    notes = [
        CANONICAL_MODEL_JUSTIFICATION,
        f"Canonical extrap: {canonical.name} → {extrap_10m_128:.0f} ms @ 10M×128 "
        f"(~{ratio:.1f}× tpuf {tpuf_p50} ms). Not validated on AWS or 1024-d.",
        f"Diagnostic best-by-R² on collapsed tiers: {best.name} (R²={best.r2:.4f}).",
    ]
    if best.name != canonical.name:
        alt = best.predict(N_REF)
        notes.append(
            f"SUPERSEDED for reports: auto best-model {best.name} would give "
            f"~{alt:.0f} ms @ 10M×128 (~{alt / tpuf_p50:.1f}× tpuf)—do not mix "
            "with canonical linear ratio in the same headline."
        )
    notes.extend(
        [
            "SUPERSEDED: log_linear on 111/525/813 ms sweep → ~2160 ms @ 10M×128 "
            "(~2.5× tpuf)—tiers refreshed to 96/412/880.",
            "SUPERSEDED: anecdotal ~7 s @ 100k or ~70× @ 10M from debug build or "
            "host contention—not in committed op-scaling JSON.",
            "100k query_latencies_ms spread 813–900 in latest JSON (p50=880); prior "
            "stability runs 813/857/906 ms (σ≈47 ms).",
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


@dataclass(frozen=True)
class ComparisonSnapshot:
    tpuf_p50: int
    tpuf_warm_p50: int
    measured_tiers: list[tuple[int, float]]  # (N, p50_ms) primary tiers only
    warm_tiers: list[tuple[int, float]]  # (N, p50_ms) warm tiers only
    warm_ratios_vs_tpuf: dict[int, float]  # N → op warm p50 / tpuf warm p50
    extrap_10m_128: float
    extrap_10m_sqrt: float
    canonical_model: str
    power_beta: float
    ratio_vs_tpuf: float
    confidence: str


def compute_comparison(model_override: str | None = None) -> ComparisonSnapshot:
    """Load committed JSON, fit models, return summary for verdict/charts."""
    tpuf_p50 = load_tpuf_cold_p50()
    tpuf_warm_p50 = load_tpuf_warm_p50()
    points = load_op_scaling_points()
    warm_points = load_op_warm_points()
    collapsed = collapse_by_n(points)
    models = [
        fit_power_law(collapsed),
        fit_linear(collapsed),
        fit_log_linear(collapsed),
    ]
    canonical = resolve_canonical_model(models, model_override)
    power = next(m for m in models if m.name == "power_law")
    primary = [(n, ms) for n, ms in collapsed if n in (10_000, 50_000, 100_000)]
    primary.sort(key=lambda t: t[0])
    warm_tiers = [(p.n, p.p50_ms) for p in warm_points]
    extrap_128 = canonical.predict(N_REF)
    return ComparisonSnapshot(
        tpuf_p50=tpuf_p50,
        tpuf_warm_p50=tpuf_warm_p50,
        measured_tiers=primary,
        warm_tiers=warm_tiers,
        warm_ratios_vs_tpuf=warm_ratios_vs_tpuf(warm_points, tpuf_warm_p50),
        extrap_10m_128=extrap_128,
        extrap_10m_sqrt=dim_scale_sqrt(extrap_128),
        canonical_model=canonical.name,
        power_beta=power.params["b"],
        ratio_vs_tpuf=extrap_128 / tpuf_p50,
        confidence=extrap_confidence(),
    )


def operator_verdict_paragraph(
    snap: ComparisonSnapshot | None = None, model_override: str | None = None
) -> str:
    """Single paragraph for operators (stdout from --verdict-only)."""
    s = snap or compute_comparison(model_override)
    tier_str = " / ".join(f"{fmt_n(n)}={ms:.0f}ms" for n, ms in s.measured_tiers)
    p100 = next((ms for n, ms in s.measured_tiers if n == 100_000), None)
    ratio_100k_tpuf = (p100 / s.tpuf_p50) if p100 is not None else None
    coincidence = ""
    if p100 is not None:
        ratio_note = (
            f" (~{ratio_100k_tpuf:.2f}×)" if ratio_100k_tpuf is not None else ""
        )
        coincidence = (
            f"Critical: openpuffer measured {fmt_n(100_000)}×128 cold p50 "
            f"~{p100:.0f} ms is the same order of magnitude as turbopuffer official "
            f"{s.tpuf_p50} ms @ 10M×1024{ratio_note}—**not comparable** "
            f"(100× fewer docs, 8× fewer dims, MinIO vs GCP fleet, different load); "
        )
    ratio_sqrt = s.extrap_10m_sqrt / s.tpuf_p50
    ballpark = ballpark_verdict(s.extrap_10m_sqrt, s.tpuf_p50)
    tpuf_commit_ms = load_tpuf_write_commit_ms_claim()
    ingest_rows = load_op_ingest_throughput()
    ingest_clause = ""
    if ingest_rows:
        parts = [
            f"{fmt_n(n)}={dps:.0f} docs/s ({wall:.0f}s wall)"
            for n, wall, dps in ingest_rows
        ]
        ingest_clause = (
            f" openpuffer MinIO ingest+index throughput is {', '.join(parts)} "
            f"(WAL-limited upsert+index wait) vs turbopuffer's published "
            f"≤~{tpuf_commit_ms} ms durable write-commit latency (not the same throughput model);"
        )
    warm_clause = ""
    if s.warm_tiers:
        warm_parts = [
            f"{fmt_n(n)} warm={ms:.0f}ms (~{s.warm_ratios_vs_tpuf[n]:.1f}× tpuf {s.tpuf_warm_p50}ms @ 10M)"
            for n, ms in s.warm_tiers
            if n in s.warm_ratios_vs_tpuf
        ]
        warm_clause = (
            f" Warm path (POST /warm + eventual, MinIO disk cache): {', '.join(warm_parts)}—"
            f"**not comparable** to tpuf fleet NVMe @ 10M×1024;"
        )
    return (
        f"{coincidence}if doc-count scaling on this harness held to 10M, canonical "
        f"{s.canonical_model} extrapolation gives ~{s.extrap_10m_128:.0f} ms @ 10M×128 "
        f"(~{s.ratio_vs_tpuf:.1f}× tpuf cold {s.tpuf_p50} ms—{s.confidence} confidence, unmeasured); "
        f"tiers ({tier_str}) imply power-law β≈{s.power_beta:.2f}, "
        f"√dim heuristic ~{s.extrap_10m_sqrt:.0f} ms (~{ratio_sqrt:.0f}× cold).{warm_clause}{ingest_clause} "
        f"{ballpark}. Treat as scaling-shape signal only—do not read 100k≈874 ms as parity; "
        "10M openpuffer is unmeasured."
    )


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
    canonical: ModelFit,
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
        tags: list[str] = []
        if m.name == canonical.name:
            tags.append("canonical")
        if m.name == best.name:
            tags.append("best R²")
        suffix = f" **{', '.join(tags)}**" if tags else ""
        lines.append(
            f"| {m.name}{suffix} | {m.formula} | {m.r2:.4f} | {m.rmse_ms:.1f} |"
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
            f"**Canonical extrapolation model:** `{canonical.name}` — {canonical.formula}",
            f"**Best model by R² (diagnostic):** `{best.name}` — {best.formula}",
            "",
            f"| Extrapolation @ 10M×128 ({canonical.name}) | {extrap_10m_128:.0f} ms |",
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


def dry_run_compare(model_override: str | None = None) -> int:
    """Offline plan: paths that would be read and summary ratios (no file writes)."""
    read_paths: list[Path] = [TPUF_REF]
    read_paths.extend(sorted(RESULTS.glob("op-scaling-*.json")))
    read_paths.extend(sorted(RESULTS.glob("op-scaling-*-warm.json")))
    # glob above may duplicate *-warm; dedupe while preserving order
    seen: set[Path] = set()
    unique_paths: list[Path] = []
    for p in read_paths:
        if p in seen:
            continue
        seen.add(p)
        unique_paths.append(p)

    print("compare-op-scaling dry-run OK (committed JSON only; no writes)")
    print(f"  results_dir: {RESULTS.relative_to(ROOT)}")
    print("  would_read:")
    for p in unique_paths:
        rel = p.relative_to(ROOT)
        status = "present" if p.is_file() else "MISSING"
        print(f"    - {rel} ({status})")

    missing = [p for p in unique_paths if not p.is_file()]
    if missing:
        print(
            f"  warning: {len(missing)} input file(s) missing — full compare may fail",
            file=sys.stderr,
        )
        print("  summary_ratios_source: skipped (input files missing)")
        return 1

    try:
        if SUMMARY_PATH.is_file():
            summary = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
            ratios = summary.get("ratios", {})
            canon = summary.get("canonical_extrapolation", {})
            print(f"  summary_ratios_source: {SUMMARY_PATH.relative_to(ROOT)} (committed)")
        else:
            summary = build_scaling_comparison_summary(model_override)
            ratios = summary.get("ratios", {})
            canon = summary.get("canonical_extrapolation", {})
            print(
                "  summary_ratios_source: computed from op-scaling-*.json "
                f"(no committed {SUMMARY_PATH.name})"
            )
    except SystemExit as exc:
        print(f"  summary_ratios_source: unavailable ({exc})", file=sys.stderr)
        return 1

    tpuf_block = summary.get("tpuf_official") or load_tpuf_official_block()
    print("  tpuf_official:")
    print(json.dumps(tpuf_block, indent=2, sort_keys=True))

    print("  summary_ratios:")
    print(json.dumps(ratios, indent=2, sort_keys=True))
    if canon:
        print("  canonical_extrapolation:")
        print(json.dumps(canon, indent=2, sort_keys=True))
    return 0


def main() -> int:
    model_override = parse_model_arg(sys.argv)
    if "--dry-run" in sys.argv or "-n" in sys.argv:
        return dry_run_compare(model_override)

    if "--verdict-only" in sys.argv:
        print(operator_verdict_paragraph(model_override=model_override))
        return 0

    if "--write-summary" in sys.argv:
        other_flags = [
            a
            for a in sys.argv[1:]
            if a.startswith("--")
            and a not in ("--write-summary",)
            and not a.startswith("--model=")
        ]
        if not other_flags:
            summary_path = write_scaling_comparison_summary(model_override)
            print(f"Wrote dashboard summary: {summary_path.relative_to(ROOT)}")
            return 0

    if "--csv" in sys.argv:
        other_flags = [
            a
            for a in sys.argv[1:]
            if a.startswith("--")
            and a not in ("--csv",)
            and not a.startswith("--model=")
        ]
        if not other_flags:
            csv_path = write_scaling_comparison_csv(model_override)
            print(f"Wrote analysis CSV: {csv_path.relative_to(ROOT)}")
            return 0

    tpuf_p50 = load_tpuf_cold_p50()
    tpuf_warm_p50 = load_tpuf_warm_p50()
    points = load_op_scaling_points()
    warm_points = load_op_warm_points()
    warm_ratio_map = warm_ratios_vs_tpuf(warm_points, tpuf_warm_p50)
    collapsed = collapse_by_n(points)

    models = [
        fit_power_law(collapsed),
        fit_linear(collapsed),
        fit_log_linear(collapsed),
    ]
    best = pick_best_model(models)
    canonical = resolve_canonical_model(models, model_override)
    loo_tiers = leave_one_out_2fit_tiers(collapsed)
    loo_4pt = leave_one_out_4point(points)

    extrap_1m = canonical.predict(N_1M)
    extrap_10m_128 = canonical.predict(N_REF)
    extrap_10m_sqrt = dim_scale_sqrt(extrap_10m_128)
    extrap_10m_linear_d = dim_scale_linear(extrap_10m_128)
    dim_factor_sqrt = math.sqrt(DIM_REF / DIM_OP)
    dim_factor_linear = DIM_REF / DIM_OP
    ratio_vs_tpuf = extrap_10m_128 / tpuf_p50
    confidence = extrap_confidence()

    backsolve = {
        m.name: backsolve_n_for_target(m, float(tpuf_p50)) for m in models
    }
    backsolve_canonical = backsolve[canonical.name]

    print("=== openpuffer scaling → turbopuffer 10M reference ===\n")
    print(f"tpuf official cold p50: {tpuf_p50} ms (10M × 1024, GCP, 8 QPS × 30m)")
    print(f"tpuf official warm p50: {tpuf_warm_p50} ms (10M × 1024, hint_cache_warm)\n")

    print("Measured openpuffer cold p50 (MinIO, release + v3):")
    for p in points:
        print(f"  {p.n:>7} docs × 128-d ({p.label}): {p.p50_ms:.0f} ms")
    if warm_points:
        print("\nMeasured openpuffer warm p50 (POST /warm + eventual, disk cache):")
        for p in warm_points:
            ratio = warm_ratio_map.get(p.n)
            ratio_s = f" (~{ratio:.1f}× tpuf warm {tpuf_warm_p50} ms)" if ratio else ""
            print(f"  {p.n:>7} docs × 128-d ({p.label}): {p.p50_ms:.0f} ms{ratio_s}")
    print(f"\nCollapsed tiers for regression (mean @ duplicate N): {collapsed}\n")

    print("### Model comparison (fit on collapsed tiers)")
    print("| Model | Formula | R² | RMSE (ms) |")
    print("|-------|---------|-----|-----------|")
    for m in sorted(models, key=lambda x: -x.r2):
        marks: list[str] = []
        if m.name == canonical.name:
            marks.append("canonical")
        if m.name == best.name:
            marks.append("best R²")
        suffix = f" ← {', '.join(marks)}" if marks else ""
        print(f"| {m.name}{suffix} | {m.formula} | {m.r2:.4f} | {m.rmse_ms:.1f} |")
    print()
    print(
        f"Canonical extrapolation model: **{canonical.name}** "
        f"(override: --model=linear|power_law|log_linear)\n"
    )

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

    print(f"Canonical model: **{canonical.name}** — {canonical.formula}")
    print(f"Best by R² (diagnostic): **{best.name}** — {best.formula}\n")
    print("| Scale | p50 (ms) | Notes |")
    print("|-------|----------|-------|")
    print(f"| extrap 1M × 128 | **{extrap_1m:.0f}** | {canonical.name} (canonical) |")
    print(f"| extrap 10M × 128 | **{extrap_10m_128:.0f}** | {canonical.name} (canonical) |")
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
        f"| openpuffer (extrapolated) | 10M × 128 | MinIO ({canonical.name}) | "
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
    print(f"Canonical 10M×128 / tpuf cold: {ratio_vs_tpuf:.1f}× (confidence: {confidence})")
    print(f"√dim 10M×1024 / tpuf cold: {extrap_10m_sqrt / tpuf_p50:.1f}×")
    print(f"Linear-d 10M×1024 / tpuf cold: {extrap_10m_linear_d / tpuf_p50:.1f}×")
    if warm_points:
        print("\n### Side-by-side (warm p50)")
        print("| System | Docs × dims | Environment | p50 (ms) | vs tpuf warm |")
        print("|--------|-------------|-------------|----------|--------------|")
        print(
            f"| turbopuffer (official) | 10M × 1024 | GCP managed | "
            f"**{tpuf_warm_p50}** | 1.0× |"
        )
        for p in warm_points:
            ratio = warm_ratio_map[p.n]
            print(
                f"| openpuffer (measured) | {p.n:,} × 128 | MinIO warm | "
                f"**{p.p50_ms:.0f}** | **{ratio:.1f}×** |"
            )
        print(
            "\nWarm ratios use tpuf official 14 ms @ 10M×1024; openpuffer tiers are "
            "10k/100k × 128 on MinIO—not comparable N/D/backend."
        )

    md = markdown_appendix(
        points,
        models,
        canonical,
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

    extrap_notes = build_extrap_notes(
        points, canonical, best, extrap_10m_128, tpuf_p50
    )
    extrap_json = {
        "canonical_model": canonical.name,
        "extrap_p50_10m_128_ms": round(extrap_10m_128),
        "ratio_vs_tpuf": round(ratio_vs_tpuf, 2),
        "confidence": confidence,
        "notes": extrap_notes,
        "fit": {
            "canonical_model": canonical.name,
            "best_model_by_r2": best.name,
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
        "tpuf_official_warm_p50_ms": tpuf_warm_p50,
        "warm_measured_points": [
            {"n": p.n, "p50_ms": p.p50_ms, "label": p.label} for p in warm_points
        ],
        "warm_ratios_vs_tpuf": {
            str(n): round(r, 2) for n, r in sorted(warm_ratio_map.items())
        },
        "ratio_warm_10k_vs_tpuf": round(warm_ratio_map[10_000], 2)
        if 10_000 in warm_ratio_map
        else None,
        "ratio_warm_100k_vs_tpuf": round(warm_ratio_map[100_000], 2)
        if 100_000 in warm_ratio_map
        else None,
        "ratio_heuristic_vs_tpuf": round(extrap_10m_sqrt / tpuf_p50, 2),
        "ratio_linear_d_vs_tpuf": round(extrap_10m_linear_d / tpuf_p50, 2),
        "backsolve_n_at_tpuf_p50": {
            k: (round(v) if v and v > 0 else None) for k, v in backsolve.items()
        },
        "backsolve_n_canonical_model": (
            round(backsolve_canonical)
            if backsolve_canonical and backsolve_canonical > 0
            else None
        ),
        "ms_per_doc_us_extrap_10m": round(ms_per_doc_op * 1e6, 3),
        "ms_per_doc_us_tpuf_10m": round(ms_per_doc_tpuf * 1e6, 3),
    }
    print()
    print("EXTRAP_JSON=" + json.dumps(extrap_json))

    summary_path = write_scaling_comparison_summary(model_override)
    print(f"Wrote dashboard summary: {summary_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())