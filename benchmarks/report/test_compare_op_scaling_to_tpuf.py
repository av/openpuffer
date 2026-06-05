"""Unit tests for power-law extrapolation helper."""

from __future__ import annotations

import csv
import io
import json
import math
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import pytest

import compare_op_scaling_to_tpuf as compare_mod
from compare_op_scaling_to_tpuf import (
    CANONICAL_MODEL,
    CSV_COLUMNS,
    SUMMARY_SCHEMA_VERSION,
    ballpark_verdict,
    build_scaling_comparison_csv_rows,
    build_scaling_comparison_summary,
    compute_comparison,
    dim_scale_sqrt,
    dry_run_compare,
    fit_power_law,
    load_op_warm_points,
    load_tpuf_warm_p50,
    operator_verdict_paragraph,
    parse_model_arg,
    warm_ratios_vs_tpuf,
    write_scaling_comparison_csv,
)


def test_fit_power_law_near_linear() -> None:
    points = [(10_000, 100.0), (50_000, 500.0), (100_000, 1000.0)]
    fit = fit_power_law(points)
    b = fit.params["b"]
    a = fit.params["a"]
    assert 0.95 <= b <= 1.05
    pred = a * (1_000_000**b)
    assert 9_000 <= pred <= 11_000


def test_dim_scale_sqrt() -> None:
    assert dim_scale_sqrt(1000.0) == 1000.0 * math.sqrt(8)


def test_ballpark_verdict_slow() -> None:
    msg = ballpark_verdict(50_000.0, 874)
    assert "slower" in msg or "not" in msg


def test_operator_verdict_paragraph() -> None:
    snap = compute_comparison()
    para = operator_verdict_paragraph(snap)
    assert "874" in para
    assert "not comparable" in para.lower()
    assert "100.0k" in para or "100k" in para.lower()
    assert "10M" in para or "10.0M" in para
    assert len(para.split(".")) >= 3
    assert "200" in para or "docs/s" in para
    assert snap.canonical_model == CANONICAL_MODEL
    assert "canonical" in para
    assert snap.ratio_vs_tpuf > 50


def test_canonical_model_linear_on_committed_json() -> None:
    snap = compute_comparison()
    assert snap.canonical_model == "linear"
    assert 80_000 <= snap.extrap_10m_128 <= 95_000
    assert 95 <= snap.ratio_vs_tpuf <= 105
    assert snap.confidence == "low"


def test_build_scaling_comparison_summary() -> None:
    summary = build_scaling_comparison_summary()
    assert summary["schema_version"] == SUMMARY_SCHEMA_VERSION
    assert summary["tpuf_official"]["cold"]["p50_ms"] == 874
    assert summary["tpuf_official"]["warm"]["p50_ms"] == 14
    assert len(summary["openpuffer_measured"]) >= 5
    canon = summary["canonical_extrapolation"]
    assert canon["model"] == "linear"
    assert 80_000 <= canon["p50_ms"] <= 95_000
    assert summary["ratios"]["cold_10m_128_vs_tpuf_cold"] == canon["ratio_vs_tpuf_cold"]
    ingest = summary["ratios"]["ingest_docs_per_sec"]
    assert ingest["10000"] == pytest.approx(909.09, rel=0.01)
    assert ingest["50000"] == pytest.approx(3571.43, rel=0.01)
    assert ingest["100000"] == pytest.approx(757.58, rel=0.01)
    assert summary["confidence"] == "low"
    assert "874" in summary["verdict_text"]
    assert summary["verdict_text"] == operator_verdict_paragraph(compute_comparison())


def test_build_scaling_comparison_csv_rows() -> None:
    rows = build_scaling_comparison_csv_rows()
    assert len(rows) == 9  # 2 tpuf + 6 measured + 1 extrap
    assert list(rows[0].keys()) == list(CSV_COLUMNS)
    tpuf_cold = next(
        r for r in rows if r["system"] == "turbopuffer" and r["cache"] == "cold"
    )
    assert tpuf_cold["tier"] == "10m"
    assert tpuf_cold["docs"] == "10000000"
    assert tpuf_cold["dims"] == "1024"
    assert tpuf_cold["p50"] == "874"
    assert tpuf_cold["extrapolated"] == "false"
    extrap = next(r for r in rows if r["extrapolated"] == "true")
    assert extrap["system"] == "openpuffer"
    assert extrap["tier"] == "10m-extrap"
    assert extrap["docs"] == "10000000"
    assert extrap["dims"] == "128"
    assert extrap["cache"] == "cold"
    assert extrap["p90"] == ""
    assert extrap["p99"] == ""
    assert 80_000 <= int(extrap["p50"]) <= 95_000
    assert extrap["source"].startswith("compare_op_scaling_to_tpuf.py:linear@10M")
    measured = [r for r in rows if r["system"] == "openpuffer" and r["extrapolated"] == "false"]
    assert len(measured) == 6
    assert {r["tier"] for r in measured} == {
        "10k",
        "10k-synthetic128",
        "50k",
        "100k",
        "10k-warm",
        "100k-warm",
    }


def test_write_scaling_comparison_csv(tmp_path: Path) -> None:
    out = tmp_path / "scaling-comparison.csv"
    write_scaling_comparison_csv(path=out)
    with out.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames == list(CSV_COLUMNS)
        rows = list(reader)
    assert len(rows) == 9
    assert sum(1 for r in rows if r["extrapolated"] == "true") == 1


def test_warm_metrics_on_committed_json() -> None:
    assert load_tpuf_warm_p50() == 14
    warm = load_op_warm_points()
    assert len(warm) >= 2
    ratios = warm_ratios_vs_tpuf(warm, 14)
    assert 7 <= ratios[10_000] <= 9
    assert 55 <= ratios[100_000] <= 65
    snap = compute_comparison()
    assert snap.tpuf_warm_p50 == 14
    assert snap.warm_ratios_vs_tpuf[10_000] == ratios[10_000]
    para = operator_verdict_paragraph(snap)
    assert "warm=" in para.lower() or "warm " in para.lower()
    assert "14" in para


def _minimal_tpuf_ref() -> dict:
    return {
        "workload": {"document_count": 10_000_000, "dimensions": 1024},
        "latencies_ms": {
            "cold": {"p50": 874, "p90": 900, "p99": 900},
            "warm": {"p50": 14, "p90": 20, "p99": 25},
        },
        "write_path": {"durable_commit_latency_ms_claim": 200},
    }


def _minimal_op_scaling(
    *,
    label: str,
    docs: int,
    p50_ms: int,
    path: str = "cold",
) -> dict:
    return {
        "path": path,
        "namespace_docs": docs,
        "dimensions": 128,
        "p50_ms": p50_ms,
        "p90_ms": p50_ms + 10,
        "p99_ms": p50_ms + 20,
        "ingest_wall_secs": 10.0,
        "docs_per_sec": docs / 10.0,
    }


def test_read_cold_json_missing_file_returns_none(tmp_path: Path) -> None:
    assert compare_mod._read_cold_json(tmp_path / "op-scaling-missing.json") is None


def test_dry_run_missing_op_scaling_files(tmp_path: Path, monkeypatch) -> None:
    results = tmp_path / "results"
    results.mkdir()
    tpuf_path = results / "tpuf-official-reference.json"
    tpuf_path.write_text(json.dumps(_minimal_tpuf_ref()), encoding="utf-8")
    (results / "op-scaling-10k.json").write_text(
        json.dumps(_minimal_op_scaling(label="10k", docs=10_000, p50_ms=96)),
        encoding="utf-8",
    )

    monkeypatch.setattr(compare_mod, "ROOT", tmp_path)
    monkeypatch.setattr(compare_mod, "RESULTS", results)
    monkeypatch.setattr(compare_mod, "TPUF_REF", tpuf_path)
    monkeypatch.setattr(
        compare_mod,
        "SYNTH128_PATH",
        results / "op-scaling-10k-synthetic128.json",
    )
    monkeypatch.setattr(compare_mod, "SUMMARY_PATH", results / "scaling-comparison-summary.json")

    stdout = io.StringIO()
    stderr = io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        rc = dry_run_compare()

    out = stdout.getvalue()
    err = stderr.getvalue()
    assert rc == 1
    assert "unavailable" in err.lower() or "need ≥3" in err
    assert "summary_ratios_source: skipped" not in out


def test_dry_run_missing_tpuf_reference(tmp_path: Path, monkeypatch) -> None:
    results = tmp_path / "results"
    results.mkdir()
    tpuf_path = results / "tpuf-official-reference.json"

    monkeypatch.setattr(compare_mod, "ROOT", tmp_path)
    monkeypatch.setattr(compare_mod, "RESULTS", results)
    monkeypatch.setattr(compare_mod, "TPUF_REF", tpuf_path)
    monkeypatch.setattr(compare_mod, "SUMMARY_PATH", results / "scaling-comparison-summary.json")

    stdout = io.StringIO()
    stderr = io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        rc = dry_run_compare()

    out = stdout.getvalue()
    err = stderr.getvalue()
    assert rc == 1
    assert "MISSING" in out
    assert "tpuf-official-reference.json" in out
    assert "missing" in err.lower()
    assert "skipped (input files missing)" in out


def test_canonical_model_power_law_override() -> None:
    assert parse_model_arg(["--model=power_law"]) == "power_law"
    snap = compute_comparison(model_override="power_law")
    assert snap.canonical_model == "power_law"
    assert 60_000 <= snap.extrap_10m_128 <= 75_000
    assert 70 <= snap.ratio_vs_tpuf <= 80
    linear = compute_comparison()
    assert linear.canonical_model == CANONICAL_MODEL
    assert snap.extrap_10m_128 < linear.extrap_10m_128
    summary = build_scaling_comparison_summary(model_override="power_law")
    assert summary["canonical_extrapolation"]["model"] == "power_law"


def test_scaling_comparison_csv_row_count() -> None:
    rows = build_scaling_comparison_csv_rows()
    assert len(rows) == 9
    rows_pl = build_scaling_comparison_csv_rows(model_override="power_law")
    assert len(rows_pl) == 9
    assert sum(1 for r in rows if r["system"] == "turbopuffer") == 2
    assert sum(1 for r in rows if r["extrapolated"] == "true") == 1


def test_dry_run_compare_output_contains_tpuf_874() -> None:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        rc = dry_run_compare()

    out = stdout.getvalue()
    assert rc == 0
    assert "compare-op-scaling dry-run OK" in out
    assert "874" in out
    assert "tpuf_official" in out
    assert "summary_ratios" in out
    assert stderr.getvalue() == ""