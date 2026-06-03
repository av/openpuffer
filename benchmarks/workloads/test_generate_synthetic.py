"""Unit tests for benchmarks/workloads/generate_synthetic.py (no network)."""

from __future__ import annotations

import json
import math
import subprocess
import sys
from pathlib import Path

import pytest

WORKLOADS_DIR = Path(__file__).resolve().parent
GENERATOR = WORKLOADS_DIR / "generate_synthetic.py"

sys.path.insert(0, str(WORKLOADS_DIR))
import generate_synthetic as gen  # noqa: E402


def test_bench_sin_matches_rust_formula() -> None:
    """Same formula as tests/bench_cold.rs synthetic_embedding."""
    for doc_index in (0, 1, 99, 999, 99_999):
        expected = [math.sin((doc_index * 128 + d) * 0.001) for d in range(128)]
        assert gen.bench_sin_embedding(doc_index, 128) == expected


def test_deterministic_prng_embedding() -> None:
    cfg = gen.WorkloadConfig(
        seed=42,
        num_docs=1000,
        dim=128,
        batch_size=100,
        id_scheme="doc-prefix",
        embedding_fn="xorshift_f32",
    )
    a = gen.embedding_for_doc(cfg, 500)
    b = gen.embedding_for_doc(cfg, 500)
    c = gen.embedding_for_doc(cfg, 501)
    assert a == b
    assert a != c


def test_upsert_batch_shape() -> None:
    cfg = gen.WorkloadConfig(
        seed=1,
        num_docs=25_000,
        dim=128,
        batch_size=10_000,
        id_scheme="doc-prefix",
        embedding_fn="bench_sin_v1",
    )
    cols = gen.upsert_columns_batch(cfg, 10_000, 10_000)
    assert len(cols["id"]) == 10_000
    assert cols["id"][0] == "doc-10000"
    assert len(cols["embedding"][0]) == 128
    assert cols["category"][0] == "cat-0"
    assert cols["priority"][0] == 0


def test_manifest_and_queries_counts() -> None:
    cfg = gen.WorkloadConfig(
        seed=42,
        num_docs=100_000,
        dim=128,
        batch_size=10_000,
        id_scheme="doc-prefix",
        embedding_fn="bench_sin_v1",
    )
    manifest = gen.manifest_dict(cfg)
    assert manifest["num_docs"] == 100_000
    assert manifest["num_batches"] == 10
    assert manifest["embedding_fn"] == "bench_sin_v1"

    queries = gen.queries_dict(cfg)
    assert len(queries["vector_queries"]) == 50
    assert len(queries["filter_queries"]) == 6
    assert len(queries["hybrid_queries"]) == 4
    assert queries["vector_queries"][0]["doc_index"] == 0
    assert queries["cold_query_protocol"]["runs"] == 7
    assert queries["spot_check"]["count"] == 10
    assert queries["spot_check"]["top_k"] == 10


def test_openpuffer_first_batch_has_schema() -> None:
    cfg = gen.WorkloadConfig(
        seed=42,
        num_docs=1000,
        dim=128,
        batch_size=500,
        id_scheme="doc-prefix",
        embedding_fn="bench_sin_v1",
    )
    first = gen.openpuffer_write_body(cfg, 0, 500, include_schema=True)
    second = gen.openpuffer_write_body(cfg, 500, 500, include_schema=False)
    assert "schema" in first
    assert "distance_metric" in first["schema"]
    assert "schema" not in second
    assert len(first["upsert_columns"]["id"]) == 500


def test_cli_writes_manifest_and_queries(tmp_path: Path) -> None:
    out = tmp_path / "tier"
    proc = subprocess.run(
        [
            sys.executable,
            str(GENERATOR),
            "--output-dir",
            str(out),
            "--num-docs",
            "20000",
            "--batch-size",
            "10000",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0
    manifest = json.loads((out / "manifest.json").read_text())
    queries = json.loads((out / "queries.json").read_text())
    assert manifest["num_docs"] == 20_000
    assert manifest["num_batches"] == 2
    assert len(queries["vector_queries"]) == 50


def _assert_committed_manifest_matches_generator(tier_dir: Path) -> None:
    if not (tier_dir / "manifest.json").is_file():
        pytest.skip(f"{tier_dir.name} manifest not generated yet")
    committed = json.loads((tier_dir / "manifest.json").read_text())
    cfg = gen.WorkloadConfig(
        seed=committed["seed"],
        num_docs=committed["num_docs"],
        dim=committed["dim"],
        batch_size=committed["batch_size"],
        id_scheme=committed["id_scheme"],
        embedding_fn=committed["embedding_fn"],
    )
    assert gen.manifest_dict(cfg) == committed
    queries = json.loads((tier_dir / "queries.json").read_text())
    assert queries == gen.queries_dict(cfg)


@pytest.mark.parametrize(
    "subdir,num_docs",
    [
        ("l1-100k", 100_000),
        ("l2-500k", 500_000),
        ("l3-1m", 1_000_000),
    ],
)
def test_committed_tier_manifest_matches_generator(subdir: str, num_docs: int) -> None:
    """Guardrail: committed synthetic-128 tier manifests stay in sync with generator."""
    tier_dir = WORKLOADS_DIR / "synthetic-128" / subdir
    _assert_committed_manifest_matches_generator(tier_dir)
    committed = json.loads((tier_dir / "manifest.json").read_text())
    assert committed["num_docs"] == num_docs