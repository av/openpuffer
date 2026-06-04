"""Offline gate: committed op-scaling JSON matches op_scaling_v1 schema."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "benchmarks" / "results"
VALIDATE = ROOT / "scripts" / "validate-benchmark-json.sh"


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