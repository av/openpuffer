"""Large-dataset harness result JSON schema_version (shared by tpuf driver and cross_check)."""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_VERSION_FILE = Path(__file__).resolve().parent / "LARGE_BENCHMARK_JSON_SCHEMA_VERSION"


def large_benchmark_json_schema_version() -> str:
    return _VERSION_FILE.read_text(encoding="utf-8").strip()


__all__ = ["large_benchmark_json_schema_version", "_REPO_ROOT"]