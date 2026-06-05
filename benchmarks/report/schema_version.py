"""Benchmark result JSON schema_version helpers (canonical version files under benchmarks/report/)."""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_REPORT_DIR = Path(__file__).resolve().parent
_LARGE_VERSION_FILE = _REPORT_DIR / "LARGE_BENCHMARK_JSON_SCHEMA_VERSION"
_OP_SCALING_VERSION_FILE = _REPORT_DIR / "OP_SCALING_JSON_SCHEMA_VERSION"


def large_benchmark_json_schema_version() -> str:
    return _LARGE_VERSION_FILE.read_text(encoding="utf-8").strip()


def op_scaling_json_schema_version() -> str:
    return _OP_SCALING_VERSION_FILE.read_text(encoding="utf-8").strip()


__all__ = [
    "large_benchmark_json_schema_version",
    "op_scaling_json_schema_version",
    "_REPO_ROOT",
]