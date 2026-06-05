"""Offline gate: committed op-scaling JSON matches op_scaling_v1 schema."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from schema_version import op_scaling_json_schema_version

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "benchmarks" / "results"
VALIDATE = ROOT / "scripts" / "validate-benchmark-json.sh"
BASE_100K = RESULTS / "op-scaling-100k.json"
EXPECTED_SCHEMA_VERSION = op_scaling_json_schema_version()


def _run_validate(path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(VALIDATE), str(path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def _mutated_100k(tmp_path: Path, p50_ms: int) -> Path:
    data = json.loads(BASE_100K.read_text(encoding="utf-8"))
    lat = [p50_ms] * 7
    data["p50_ms"] = p50_ms
    data["p90_ms"] = p50_ms
    data["p99_ms"] = p50_ms
    data["query_latencies_ms"] = lat
    out = tmp_path / "op-scaling-100k.json"
    out.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    return out


def test_op_scaling_100k_outlier_gate_rejects_slow(tmp_path: Path) -> None:
    if not BASE_100K.is_file():
        pytest.skip("op-scaling-100k.json not committed")
    path = _mutated_100k(tmp_path, 7000)
    proc = _run_validate(path)
    assert proc.returncode != 0
    assert "likely resource contention - re-run" in proc.stderr + proc.stdout


def test_op_scaling_100k_outlier_gate_warns_fast(tmp_path: Path) -> None:
    if not BASE_100K.is_file():
        pytest.skip("op-scaling-100k.json not committed")
    path = _mutated_100k(tmp_path, 150)
    proc = _run_validate(path)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "suspiciously fast" in proc.stderr


def test_op_scaling_committed_schema_version() -> None:
    paths = sorted(RESULTS.glob("op-scaling-*.json"))
    assert paths, "expected benchmarks/results/op-scaling-*.json"
    for path in paths:
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data.get("schema_version") == EXPECTED_SCHEMA_VERSION, (
            f"{path.name}: schema_version {data.get('schema_version')!r} != "
            f"{EXPECTED_SCHEMA_VERSION!r}"
        )


def test_op_scaling_artifacts_validate() -> None:
    paths = sorted(RESULTS.glob("op-scaling-*.json"))
    assert paths, "expected benchmarks/results/op-scaling-*.json"
    proc = subprocess.run(
        [str(VALIDATE), *[str(p) for p in paths]],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr