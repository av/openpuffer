"""Offline tests for Phase 3.3 id overlap spot-check (no API keys)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CROSS_CHECK = Path(__file__).resolve().parent
WORKLOADS = ROOT / "benchmarks" / "workloads"
L1_QUERIES = WORKLOADS / "synthetic-128" / "l1-100k" / "queries.json"
MOCK_FIXTURE = CROSS_CHECK / "fixtures" / "overlap-l1-mock.json"
RUNNER = CROSS_CHECK / "run_spotcheck.py"

sys.path.insert(0, str(CROSS_CHECK))
import id_overlap as xcheck  # noqa: E402


@pytest.mark.parametrize(
    "tier_subdir,last_doc_index",
    [
        ("l1-100k", 18_000),
        ("l2-500k", 90_000),
        ("l3-1m", 180_000),
    ],
)
def test_committed_queries_have_spot_check(tier_subdir: str, last_doc_index: int) -> None:
    queries_path = WORKLOADS / "synthetic-128" / tier_subdir / "queries.json"
    queries = json.loads(queries_path.read_text())
    assert queries["spot_check"]["count"] == 10
    assert queries["spot_check"]["top_k"] == 10
    specs = xcheck.spot_check_query_specs(queries)
    assert len(specs) == 10
    assert specs[0]["name"] == "vector-q00"
    assert specs[-1]["name"] == "vector-q09"
    assert specs[-1]["doc_index"] == last_doc_index


def test_overlap_metrics_symmetric() -> None:
    a = [f"doc-{i}" for i in range(10)]
    b = ["doc-0", "doc-1", "doc-2", "doc-99", "doc-100"]
    m = xcheck.overlap_metrics(a, b, top_k=10)
    assert m["intersection_count"] == 3
    assert m["overlap_at_k"] == 0.3
    assert m["intersection_ids"] == ["doc-0", "doc-1", "doc-2"]


def test_openpuffer_body_includes_attributes() -> None:
    queries = json.loads(L1_QUERIES.read_text())
    spec = xcheck.spot_check_query_specs(queries)[0]
    body = xcheck.openpuffer_query_body(
        spec, top_k=10, include_attributes=True, consistency="strong"
    )
    assert body["include_attributes"] is True
    assert body["top_k"] == 10
    assert isinstance(body["rank_by"][3], list)
    assert len(body["rank_by"][3]) == 128


def test_mock_fixture_summary() -> None:
    payload = json.loads(MOCK_FIXTURE.read_text())
    assert payload["benchmark"] == "id_overlap_spotcheck"
    assert len(payload["queries"]) == 10
    assert payload["summary"]["mean_overlap_at_k"] == 0.69


def test_mock_payload_matches_production_schema() -> None:
    queries = json.loads(L1_QUERIES.read_text())
    payload = xcheck.build_mock_payload(
        tier="l1",
        workload_dir="benchmarks/workloads/synthetic-128/l1-100k",
        queries=queries,
    )
    assert payload["mode"] == "mock"
    assert payload["generated_at"].endswith("Z")
    assert payload["finished_at"] == payload["generated_at"]
    assert len(payload["queries"]) == 10
    row0 = payload["queries"][0]
    for key in (
        "top_k",
        "openpuffer_count",
        "turbopuffer_count",
        "intersection_count",
        "union_count",
        "overlap_at_k",
        "jaccard",
        "intersection_ids",
        "openpuffer_ids",
        "turbopuffer_ids",
    ):
        assert key in row0, key
    assert payload["summary"]["mean_overlap_at_k"] == 0.69


def test_run_spotcheck_dry_run() -> None:
    proc = subprocess.run(
        [sys.executable, str(RUNNER), "--tier", "l1", "--dry-run"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "id-overlap spot-check dry-run OK" in proc.stdout
    assert "vector-q09" in proc.stdout


def test_openpuffer_namespace_issues_empty() -> None:
    issues = xcheck.openpuffer_namespace_issues(
        {"wal_commit_seq": 0, "index_cursor": 0, "approx_row_count": 0},
        expected_docs=100_000,
        namespace="bench-large-l1",
    )
    assert any("wal_commit_seq=0" in i for i in issues)
    assert any("empty" in i.lower() for i in issues)


def test_openpuffer_namespace_issues_not_found() -> None:
    issues = xcheck.openpuffer_namespace_issues(
        None,
        expected_docs=100_000,
        namespace="missing-ns",
    )
    assert len(issues) == 1
    assert "not found" in issues[0]


def test_openpuffer_namespace_issues_ready() -> None:
    issues = xcheck.openpuffer_namespace_issues(
        {
            "wal_commit_seq": 10,
            "index_cursor": 10,
            "preferred_ann_version": 3,
            "approx_row_count": 100_000,
        },
        expected_docs=100_000,
        namespace="bench-large-l1",
    )
    assert issues == []


def test_turbopuffer_namespace_issues_empty() -> None:
    class Meta:
        approx_row_count = 0
        index = type("Idx", (), {"status": "up-to-date"})()

    issues = xcheck.turbopuffer_namespace_issues(
        Meta(),
        expected_docs=100_000,
        namespace="bench-tpuf-2026-06-04-l1",
    )
    assert any("approx_row_count=0" in i for i in issues)


def test_format_namespace_preflight_error_hints() -> None:
    msg = xcheck.format_namespace_preflight_error(
        engine="openpuffer",
        namespace="bench-large-l1",
        issues=["wal_commit_seq=0"],
        tier="l1",
    )
    assert "OPENPUFFER_BASE_URL" in msg
    assert "wal_commit_seq=0" in msg
    assert "bench-large-l1" in msg


def test_run_spotcheck_mock_writes_json(tmp_path: Path) -> None:
    out = tmp_path / "id-overlap-l1.json"
    proc = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--tier",
            "l1",
            "--mock",
            "--output",
            str(out),
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0
    written = json.loads(out.read_text())
    assert written["schema_version"] == "large_benchmark_v1"
    assert written["generated_at"].endswith("Z")
    assert written["finished_at"] == written["generated_at"]
    assert written["mode"] == "mock"
    assert written["summary"]["query_count"] == 10
    assert "openpuffer_ids" in written["queries"][0]