"""Tests for benchmark result UTC timestamp helpers."""

from __future__ import annotations

import pytest

from utc_timestamps import (
    benchmark_run_timestamps,
    utc_now_iso,
    validate_benchmark_timestamps,
    validate_utc_timestamp_field,
)


def test_utc_now_iso_z_suffix() -> None:
    ts = utc_now_iso()
    validate_utc_timestamp_field("generated_at", ts)
    assert ts.endswith("Z")
    assert "+" not in ts


def test_benchmark_run_timestamps_ordering() -> None:
    stamps = benchmark_run_timestamps(started_at="2026-06-04T00:00:00Z")
    validate_benchmark_timestamps(stamps)
    assert stamps["started_at"] == "2026-06-04T00:00:00Z"
    assert stamps["finished_at"] == stamps["generated_at"]


def test_validate_rejects_offset() -> None:
    with pytest.raises(ValueError, match="Z suffix"):
        validate_utc_timestamp_field("started_at", "2026-06-04T12:00:00+00:00")


def test_validate_rejects_local_without_z() -> None:
    with pytest.raises(ValueError, match="Z suffix"):
        validate_utc_timestamp_field("finished_at", "2026-06-04T12:00:00")