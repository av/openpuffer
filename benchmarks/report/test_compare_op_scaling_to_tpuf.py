"""Unit tests for power-law extrapolation helper."""

from __future__ import annotations

import math

from compare_op_scaling_to_tpuf import ballpark_verdict, dim_scale_heuristic, fit_power_law


def test_fit_power_law_near_linear() -> None:
    points = [(10_000, 100.0), (50_000, 500.0), (100_000, 1000.0)]
    fit = fit_power_law(points)
    assert 0.95 <= fit.b <= 1.05
    pred = math.exp(math.log(fit.a) + fit.b * math.log(1_000_000))
    assert 9_000 <= pred <= 11_000


def test_dim_scale_heuristic() -> None:
    assert dim_scale_heuristic(1000.0) == 1000.0 * math.sqrt(8)


def test_ballpark_verdict_slow() -> None:
    msg = ballpark_verdict(50_000.0, 874)
    assert "slower" in msg or "not" in msg