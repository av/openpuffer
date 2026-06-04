"""Unit tests for power-law extrapolation helper."""

from __future__ import annotations

import math

from compare_op_scaling_to_tpuf import (
    ballpark_verdict,
    compute_comparison,
    dim_scale_sqrt,
    fit_power_law,
    operator_verdict_paragraph,
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
    assert "10M" in para or "10.0M" in para
    assert len(para.split(".")) >= 3
    assert "200" in para or "docs/s" in para