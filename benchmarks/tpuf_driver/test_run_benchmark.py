"""Offline tests for benchmarks/tpuf_driver/run_benchmark.py (no API key)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

DRIVER_DIR = Path(__file__).resolve().parent
ROOT = DRIVER_DIR.parents[1]
sys.path.insert(0, str(DRIVER_DIR))

import run_benchmark as rb  # noqa: E402


def test_percentile_ms() -> None:
    assert rb.percentile_ms([10, 20, 30, 40, 50, 60, 70], 50) == 40
    assert rb.percentile_ms([100], 95) == 100
    assert rb.percentile_ms([], 50) == 0


def test_index_is_ready() -> None:
    meta = SimpleNamespace(
        index=SimpleNamespace(status="up-to-date"),
        approx_row_count=100_000,
    )
    ready, status, rows = rb.index_is_ready(meta, 100_000)
    assert ready is True
    assert status == "up-to-date"
    assert rows == 100_000

    meta.index.status = "updating"
    ready, _, _ = rb.index_is_ready(meta, 100_000)
    assert ready is False


def test_build_result_payload_schema() -> None:
    ctx = rb.RunContext(
        tier="l1",
        workload_dir=ROOT / "benchmarks/workloads/synthetic-128/l1-100k",
        results_path=ROOT / "benchmarks/results/tpuf-l1.json",
        region="aws-us-east-1",
        namespace="bench-tpuf-2026-06-04-l1",
        num_docs=100_000,
        dim=128,
        seed=42,
        embedding_fn="bench_sin_v1",
        batch_size=10_000,
        cold_runs=7,
        query_top_k=10,
        query_consistency="strong",
        primary_query_name="vector-q00",
        query_vector=[0.0] * 128,
        recall_num=20,
        recall_top_k=10,
        index_timeout_sec=7200,
        enforce_gates=True,
        skip_ingest=False,
        skip_delete=False,
    )
    payload = rb.build_result_payload(
        ctx,
        index_meta={"status": "up-to-date", "approx_row_count": 100_000},
        ingest_stats={
            "ingest_elapsed_secs": 120.5,
            "ingest_docs_per_sec": 830.0,
            "ingest_rows_written": 100_000,
            "ingest_batches": [],
        },
        cold_runs=[{"run": 1, "latency_ms": 12}],
        p50_ms=10,
        p95_ms=20,
        candidates_ratio=0.001,
        recall_at_10=0.99,
    )
    assert payload["benchmark"] == "cold_tpuf_l1"
    assert payload["environment"] == "turbopuffer:aws-us-east-1"
    assert payload["tier"] == "l1"
    assert payload["namespace_docs"] == 100_000
    assert payload["index_up_to_date"] is True
    assert payload["storage_roundtrips"] is None
    assert payload["p50_query_latency_ms"] == 10
    assert payload["recall_at_10"] == 0.99
    assert payload["ingest_docs_per_sec"] == 830.0
    assert len(payload["cold_runs"]) == 1


def test_build_context_from_l1_workload() -> None:
    args = SimpleNamespace(
        tier="l1",
        workload_dir=None,
        dry_run=False,
        skip_ingest=False,
        skip_delete=False,
    )
    ctx = rb.build_context(args)
    assert ctx.num_docs == 100_000
    assert ctx.primary_query_name == "vector-q00"
    assert len(ctx.query_vector) == 128
    assert ctx.cold_runs == 7
    assert ctx.recall_num == 20


def test_dry_run_cli() -> None:
    proc = subprocess.run(
        [
            sys.executable,
            str(DRIVER_DIR / "run_benchmark.py"),
            "--dry-run",
            "--tier",
            "l1",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "tpuf benchmark dry-run OK" in proc.stdout
    assert "tier=l1" in proc.stdout


def test_enforce_gates_fails_low_recall(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = rb.RunContext(
        tier="l1",
        workload_dir=ROOT / "benchmarks/workloads/synthetic-128/l1-100k",
        results_path=ROOT / "benchmarks/results/tpuf-l1.json",
        region="aws-us-east-1",
        namespace="bench-tpuf-test",
        num_docs=100,
        dim=128,
        seed=42,
        embedding_fn="bench_sin_v1",
        batch_size=10_000,
        cold_runs=1,
        query_top_k=10,
        query_consistency="strong",
        primary_query_name="vector-q00",
        query_vector=[0.0] * 128,
        recall_num=20,
        recall_top_k=10,
        index_timeout_sec=60,
        enforce_gates=True,
        skip_ingest=False,
        skip_delete=True,
    )
    payload = {"index_up_to_date": True, "recall_at_10": 0.5}
    with pytest.raises(SystemExit):
        rb.enforce_result_gates(ctx, payload)


def test_cold_query_once_uses_client_total_ms() -> None:
    perf = SimpleNamespace(
        client_total_ms=42,
        approx_namespace_size=100_000,
        exhaustive_search_count=200,
    )
    resp = SimpleNamespace(performance=perf, rows=[1, 2, 3])
    ns = MagicMock()
    ns.query.return_value = resp
    sample = rb.cold_query_once(
        ns, vector=[0.1] * 128, top_k=10, consistency="strong"
    )
    assert sample["latency_ms"] == 42
    assert sample["candidates_ratio"] == pytest.approx(0.002)
    ns.query.assert_called_once()