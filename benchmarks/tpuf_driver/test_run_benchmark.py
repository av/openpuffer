"""Offline tests for benchmarks/tpuf_driver/run_benchmark.py (no API key)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import pytest

DRIVER_DIR = Path(__file__).resolve().parent
ROOT = DRIVER_DIR.parents[1]
sys.path.insert(0, str(DRIVER_DIR))

import run_benchmark as rb  # noqa: E402

WORKLOADS_DIR = ROOT / "benchmarks" / "workloads"
sys.path.insert(0, str(WORKLOADS_DIR))
import generate_synthetic as gen  # noqa: E402


@pytest.mark.parametrize(
    "tier,expected_timeout",
    [("l1", 7200), ("l2", 10800), ("l3", 14400)],
)
def test_default_index_timeout_sec_per_tier(tier: str, expected_timeout: int) -> None:
    import os

    old = os.environ.pop("TURBOPUFFER_BENCH_INDEX_TIMEOUT_SEC", None)
    try:
        assert rb.default_index_timeout_sec(tier) == expected_timeout
    finally:
        if old is not None:
            os.environ["TURBOPUFFER_BENCH_INDEX_TIMEOUT_SEC"] = old


def test_retry_backoff_sec() -> None:
    with patch.dict(
        os.environ,
        {
            "TURBOPUFFER_INGEST_RETRY_BASE_MS": "500",
            "TURBOPUFFER_INGEST_RETRY_MAX_MS": "8000",
        },
        clear=False,
    ):
        assert rb.retry_backoff_sec(1) == 1
        assert rb.retry_backoff_sec(2) == 1
        assert rb.retry_backoff_sec(3) == 2
        assert rb.retry_backoff_sec(6) <= 8


def test_parse_ingest_start_batch_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TURBOPUFFER_INGEST_START_BATCH", raising=False)
    assert rb.parse_ingest_start_batch() == 1


def test_parse_ingest_start_batch_invalid() -> None:
    with patch.dict(os.environ, {"TURBOPUFFER_INGEST_START_BATCH": "0"}, clear=False):
        with pytest.raises(SystemExit):
            rb.parse_ingest_start_batch()


def test_is_transient_api_error_sdk_types() -> None:
    try:
        from turbopuffer import (
            APIConnectionError,
            APITimeoutError,
            InternalServerError,
            RateLimitError,
        )
    except ImportError:
        pytest.skip("turbopuffer not installed")

    req = httpx.Request("POST", "https://api.turbopuffer.com/v1/namespaces/x/write")
    assert rb.is_transient_api_error(APITimeoutError(req))
    assert rb.is_transient_api_error(
        APIConnectionError(message="reset", request=req)
    )
    assert rb.is_transient_api_error(
        RateLimitError("rate", response=MagicMock(status_code=429), body=None)
    )
    assert rb.is_transient_api_error(
        InternalServerError("err", response=MagicMock(status_code=500), body=None)
    )
    from turbopuffer import BadRequestError

    assert not rb.is_transient_api_error(
        BadRequestError("bad", response=MagicMock(status_code=400), body=None)
    )


def test_ingest_batch_count() -> None:
    assert rb.ingest_batch_count(100_000, 10_000) == 10
    assert rb.ingest_batch_count(100_001, 10_000) == 11


def test_write_batch_with_retry_succeeds_after_transient() -> None:
    ns = MagicMock()
    try:
        from turbopuffer import RateLimitError
    except ImportError:
        pytest.skip("turbopuffer not installed")

    ns.write.side_effect = [
        RateLimitError("rate", response=MagicMock(status_code=429), body=None),
        SimpleNamespace(rows_affected=10_000),
    ]
    with patch.object(rb, "time") as mock_time:
        mock_time.sleep = MagicMock()
        resp = rb.write_batch_with_retry(
            ns, {"upsert_columns": {}}, batch_num=1, batch_total=10
        )
    assert int(getattr(resp, "rows_affected", 0)) == 10_000
    assert ns.write.call_count == 2
    mock_time.sleep.assert_called_once()


def test_ingest_workload_skips_batches_before_start() -> None:
    cfg = gen.WorkloadConfig(
        seed=42,
        num_docs=30_000,
        dim=128,
        batch_size=10_000,
        id_scheme="doc-prefix",
        embedding_fn="bench_sin_v1",
    )
    ns = MagicMock()
    ns.write.return_value = SimpleNamespace(rows_affected=10_000)
    stats = rb.ingest_workload(ns, cfg, start_batch=3)
    assert ns.write.call_count == 1
    assert stats["ingest_resume"]["skipped_batches"] == 2
    assert stats["ingest_rows_written"] == 10_000
    assert stats["ingest_status"] == "ok"


def test_ingest_workload_records_failure_and_raises() -> None:
    cfg = gen.WorkloadConfig(
        seed=42,
        num_docs=10_000,
        dim=128,
        batch_size=10_000,
        id_scheme="doc-prefix",
        embedding_fn="bench_sin_v1",
    )
    ns = MagicMock()
    try:
        from turbopuffer import BadRequestError
    except ImportError:
        pytest.skip("turbopuffer not installed")

    ns.write.side_effect = BadRequestError(
        "bad",
        response=MagicMock(status_code=400),
        body=None,
    )
    with pytest.raises(rb.IngestBatchError) as exc_info:
        rb.ingest_workload(ns, cfg, start_batch=1)
    err = exc_info.value
    assert err.batch_num == 1
    assert len(err.failures) == 1
    assert err.failures[0]["transient"] is False
    assert "TURBOPUFFER_INGEST_START_BATCH=1" in err.failures[0]["message"]


def test_build_result_payload_ingest_retry_fields() -> None:
    ctx = _l1_ctx()
    payload = rb.build_result_payload(
        ctx,
        index_meta={"status": "up-to-date", "approx_row_count": 100_000},
        ingest_stats={
            "ingest_elapsed_secs": 1.0,
            "ingest_docs_per_sec": 100.0,
            "ingest_rows_written": 50_000,
            "ingest_status": "failed",
            "ingest_failures": [{"batch": 6, "transient": True}],
            "ingest_resume": {"start_batch": 1, "next_batch": 6, "total_batches": 10},
            "ingest_retry": {"max_attempts": 6, "base_backoff_ms": 500},
        },
        cold_runs=[],
        p50_ms=0,
        p95_ms=0,
        candidates_ratio=None,
        recall_at_10=0.0,
    )
    assert payload["ingest_status"] == "failed"
    assert payload["ingest_failures"][0]["batch"] == 6
    assert payload["ingest_resume"]["next_batch"] == 6


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


def _l1_ctx(**overrides: object) -> rb.RunContext:
    base = dict(
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
        delete_first=True,
        warm_mode=False,
        warm_runs=20,
        warm_query_top_k=10,
        warm_consistency="eventual",
        filter_specs=(),
        hybrid_specs=(),
    )
    base.update(overrides)
    return rb.RunContext(**base)  # type: ignore[arg-type]


def test_openpuffer_query_kwargs_resolves_vector() -> None:
    vector = [0.1, 0.2]
    query = {
        "rank_by": ["Sum", ["vector", "ANN", "embedding", "$vector"], ["BM25", "text", "term"]],
        "filters": ["category", "Eq", "cat-1"],
        "top_k": 5,
        "consistency": "strong",
    }
    kwargs = rb.openpuffer_query_kwargs(query, vector)
    assert kwargs["top_k"] == 5
    assert kwargs["filters"] == ["category", "Eq", "cat-1"]
    assert list(kwargs["rank_by"][1][3]) == vector


def test_build_result_payload_schema() -> None:
    ctx = _l1_ctx()
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
    assert payload["schema_version"] == "large_benchmark_v1"
    assert payload["generated_at"].endswith("Z")
    assert payload["started_at"].endswith("Z")
    assert payload["finished_at"] == payload["generated_at"]
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


def test_build_result_payload_warm_and_secondary() -> None:
    ctx = _l1_ctx(warm_mode=True)
    payload = rb.build_result_payload(
        ctx,
        index_meta={"status": "up-to-date", "approx_row_count": 100_000},
        ingest_stats=None,
        cold_runs=[],
        p50_ms=10,
        p95_ms=20,
        candidates_ratio=None,
        recall_at_10=0.9,
        filter_query_runs=[{"query_name": "filter-a", "query_kind": "filter", "latency_ms": 30}],
        hybrid_query_runs=[{"query_name": "hybrid-a", "query_kind": "hybrid", "latency_ms": 40}],
        warm_runs=[{"run": 1, "latency_ms": 12}],
        warm_p50_ms=12,
        warm_p95_ms=15,
    )
    assert payload["p50_warm_query_latency_ms"] == 12
    assert payload["warm_query_runs"] == 20
    assert payload["warm_protocol"] == "hint_cache_warm"
    assert len(payload["filter_query_runs"]) == 1
    assert len(payload["hybrid_query_runs"]) == 1


def test_build_context_from_l1_workload() -> None:
    args = SimpleNamespace(
        tier="l1",
        workload_dir=None,
        dry_run=False,
        warm=False,
        skip_ingest=False,
        skip_delete=False,
    )
    ctx = rb.build_context(args)
    assert ctx.num_docs == 100_000
    assert ctx.primary_query_name == "vector-q00"
    assert len(ctx.query_vector) == 128
    assert ctx.cold_runs == 7
    assert ctx.recall_num == 20
    assert len(ctx.filter_specs) == 6
    assert len(ctx.hybrid_specs) == 4
    assert ctx.warm_runs == 20
    assert ctx.warm_consistency == "eventual"


def test_build_context_warm_flag() -> None:
    args = SimpleNamespace(
        tier="l1",
        workload_dir=None,
        dry_run=False,
        warm=True,
        skip_ingest=False,
        skip_delete=False,
    )
    ctx = rb.build_context(args)
    assert ctx.warm_mode is True


def test_dry_run_lists_ingest_retry_settings() -> None:
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
    assert "ingest_start_batch=1" in proc.stdout
    assert "retry_max=6" in proc.stdout


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


def test_run_workload_query_specs_mock() -> None:
    ns = MagicMock()
    perf = SimpleNamespace(client_total_ms=55, approx_namespace_size=1000, exhaustive_search_count=2)
    ns.query.return_value = SimpleNamespace(performance=perf, rows=[1])
    specs = (
        {
            "name": "filter-test",
            "vector": [0.0] * 128,
            "openpuffer_query": {
                "rank_by": ["vector", "ANN", "embedding", "$vector"],
                "filters": ["priority", "Gt", 50],
                "top_k": 10,
                "consistency": "strong",
            },
        },
    )
    runs = rb.run_workload_query_specs(ns, specs, query_kind="filter")
    assert len(runs) == 1
    assert runs[0]["query_name"] == "filter-test"
    assert runs[0]["latency_ms"] == 55
    ns.query.assert_called_once()


def test_enforce_gates_fails_low_recall(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _l1_ctx(
        namespace="bench-tpuf-test",
        num_docs=100,
        cold_runs=1,
        index_timeout_sec=60,
        skip_delete=True,
    )
    payload = {"index_up_to_date": True, "recall_at_10": 0.5}
    with pytest.raises(SystemExit):
        rb.enforce_result_gates(ctx, payload)


def test_query_once_uses_client_total_ms() -> None:
    perf = SimpleNamespace(
        client_total_ms=42,
        approx_namespace_size=100_000,
        exhaustive_search_count=200,
    )
    resp = SimpleNamespace(performance=perf, rows=[1, 2, 3])
    ns = MagicMock()
    ns.query.return_value = resp
    sample = rb.query_once(
        ns,
        rank_by=("vector", "ANN", "embedding", [0.1] * 128),
        top_k=10,
        consistency="strong",
        include_attributes=False,
    )
    assert sample["latency_ms"] == 42
    assert sample["candidates_ratio"] == pytest.approx(0.002)
    ns.query.assert_called_once()


def test_dry_run_lists_filter_hybrid_counts() -> None:
    proc = subprocess.run(
        [
            sys.executable,
            str(DRIVER_DIR / "run_benchmark.py"),
            "--dry-run",
            "--tier",
            "l1",
            "--warm",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "filter_queries=6" in proc.stdout
    assert "hybrid_queries=4" in proc.stdout
    assert "warm_mode=True" in proc.stdout