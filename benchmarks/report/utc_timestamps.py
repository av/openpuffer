"""ISO8601 UTC timestamps for large-dataset benchmark result JSON."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

UTC_ISO8601_Z_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

UTC_TIMESTAMP_FIELDS = ("generated_at", "started_at", "finished_at")


def utc_now_iso() -> str:
    """Current instant as ISO8601 UTC with Z suffix (no fractional seconds)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def benchmark_run_timestamps(*, started_at: str | None = None) -> dict[str, str]:
    """Timestamps for a completed harness run (finished_at == generated_at)."""
    finished = utc_now_iso()
    started = started_at or finished
    return {
        "started_at": started,
        "finished_at": finished,
        "generated_at": finished,
    }


def validate_utc_timestamp_field(name: str, value: Any) -> None:
    if not isinstance(value, str) or not UTC_ISO8601_Z_RE.match(value):
        raise ValueError(
            f"{name} must be ISO8601 UTC with Z suffix (YYYY-MM-DDTHH:MM:SSZ), got {value!r}"
        )


def validate_benchmark_timestamps(data: dict[str, Any]) -> None:
    for field in UTC_TIMESTAMP_FIELDS:
        if field not in data:
            raise ValueError(f"missing required timestamp {field}")
        validate_utc_timestamp_field(field, data[field])
    if data["started_at"] > data["finished_at"]:
        raise ValueError(
            f"started_at {data['started_at']!r} must be <= finished_at {data['finished_at']!r}"
        )
    if data["generated_at"] != data["finished_at"]:
        raise ValueError(
            f"generated_at must equal finished_at (got {data['generated_at']!r} vs "
            f"{data['finished_at']!r})"
        )


__all__ = [
    "UTC_ISO8601_Z_RE",
    "UTC_TIMESTAMP_FIELDS",
    "benchmark_run_timestamps",
    "utc_now_iso",
    "validate_benchmark_timestamps",
    "validate_utc_timestamp_field",
]