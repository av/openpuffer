#!/usr/bin/env python3
"""
Deterministic synthetic workload for large-dataset benchmarking (Phase 1 / A1).

Produces manifest.json, queries.json, and optional upsert batch JSON files compatible
with openpuffer (POST /v2/namespaces/{ns}) and turbopuffer (namespace.write).

Default embedding matches existing Rust harnesses (bench_cold.rs, integration_s3 stress):
  sin((doc_index * dim + d) * 0.001)

See benchmarks/workloads/synthetic-128/README.md.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Sequence

CATEGORIES = (
    "cat-0",
    "cat-1",
    "cat-2",
    "cat-3",
    "cat-4",
    "cat-5",
    "cat-6",
    "cat-7",
)

DEFAULT_DIM = 128
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_SEED = 42
DEFAULT_NUM_VECTOR_QUERIES = 50
DEFAULT_NUM_FILTER_QUERIES = 6
DEFAULT_NUM_HYBRID_QUERIES = 4
DEFAULT_SPOT_CHECK_COUNT = 10
SCHEMA_VERSION = 1


@dataclass(frozen=True)
class WorkloadConfig:
    seed: int
    num_docs: int
    dim: int
    batch_size: int
    id_scheme: str
    embedding_fn: str
    distance_metric: str = "cosine_distance"

    def doc_id(self, doc_index: int) -> str:
        if self.id_scheme == "doc-prefix":
            return f"doc-{doc_index}"
        if self.id_scheme == "u64":
            return str(doc_index)
        raise ValueError(f"unknown id_scheme: {self.id_scheme}")


def bench_sin_embedding(doc_index: int, dim: int) -> list[float]:
    """Matches tests/bench_cold.rs synthetic_embedding and integration stress upserts."""
    return [math.sin((doc_index * dim + d) * 0.001) for d in range(dim)]


def _xorshift64(state: int) -> int:
    state &= (1 << 64) - 1
    state ^= (state << 13) & ((1 << 64) - 1)
    state ^= state >> 7
    state ^= (state << 17) & ((1 << 64) - 1)
    return state & ((1 << 64) - 1)


def prng_embedding(doc_index: int, dim: int, seed: int) -> list[float]:
    """Deterministic f32-ish values in [0, 1) from seed XOR doc_index (cross-system PRNG mode)."""
    state = (seed ^ (doc_index * 0x9E3779B97F4A7C15)) & ((1 << 64) - 1)
    out: list[float] = []
    for _ in range(dim):
        state = _xorshift64(state)
        out.append((state >> 32) / float(2**32))
    return out


def embedding_for_doc(cfg: WorkloadConfig, doc_index: int) -> list[float]:
    if cfg.embedding_fn == "bench_sin_v1":
        return bench_sin_embedding(doc_index, cfg.dim)
    if cfg.embedding_fn == "xorshift_f32":
        return prng_embedding(doc_index, cfg.dim, cfg.seed)
    raise ValueError(f"unknown embedding_fn: {cfg.embedding_fn}")


def doc_attributes(cfg: WorkloadConfig, doc_index: int) -> dict[str, Any]:
    return {
        "category": CATEGORIES[doc_index % len(CATEGORIES)],
        "title": f"synthetic title {doc_index}",
        "priority": doc_index % 100,
        "text": f"stressterm document number {doc_index}",
    }


def upsert_columns_batch(cfg: WorkloadConfig, start: int, count: int) -> dict[str, Any]:
    end = start + count
    ids: list[str] = []
    embeddings: list[list[float]] = []
    categories: list[str] = []
    titles: list[str] = []
    priorities: list[int] = []
    texts: list[str] = []
    for i in range(start, end):
        ids.append(cfg.doc_id(i))
        embeddings.append(embedding_for_doc(cfg, i))
        attrs = doc_attributes(cfg, i)
        categories.append(attrs["category"])
        titles.append(attrs["title"])
        priorities.append(attrs["priority"])
        texts.append(attrs["text"])
    return {
        "id": ids,
        "embedding": embeddings,
        "category": categories,
        "title": titles,
        "priority": priorities,
        "text": texts,
    }


def openpuffer_schema(cfg: WorkloadConfig) -> dict[str, Any]:
    return {
        "text": {"type": "string", "full_text_search": True},
        "title": {"type": "string"},
        "category": {"type": "string"},
        "priority": {"type": "int"},
        "embedding": f"[{cfg.dim}]f32",
        "distance_metric": cfg.distance_metric,
    }


def openpuffer_write_body(
    cfg: WorkloadConfig, start: int, count: int, *, include_schema: bool
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "upsert_columns": upsert_columns_batch(cfg, start, count),
    }
    if include_schema:
        body["schema"] = openpuffer_schema(cfg)
    return body


def turbopuffer_write_kwargs(
    cfg: WorkloadConfig, start: int, count: int, *, include_schema: bool
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "upsert_columns": upsert_columns_batch(cfg, start, count),
    }
    if include_schema:
        out["schema"] = openpuffer_schema(cfg)
    return out


def manifest_dict(cfg: WorkloadConfig) -> dict[str, Any]:
    batches = (cfg.num_docs + cfg.batch_size - 1) // cfg.batch_size
    return {
        "schema_version": SCHEMA_VERSION,
        "workload": "synthetic-128",
        "seed": cfg.seed,
        "num_docs": cfg.num_docs,
        "dim": cfg.dim,
        "batch_size": cfg.batch_size,
        "num_batches": batches,
        "id_scheme": cfg.id_scheme,
        "id_format": "doc-{i}" if cfg.id_scheme == "doc-prefix" else "{i}",
        "embedding_fn": cfg.embedding_fn,
        "distance_metric": cfg.distance_metric,
        "attributes": {
            "category": {
                "type": "string",
                "values": list(CATEGORIES),
                "assignment": "doc_index % 8",
            },
            "title": {"type": "string", "pattern": "synthetic title {doc_index}"},
            "priority": {"type": "int", "assignment": "doc_index % 100"},
            "text": {
                "type": "string",
                "full_text_search": True,
                "pattern": "stressterm document number {doc_index}",
            },
        },
        "ingest_cadence": {
            "sleep_seconds_between_batches": 1.1,
            "note": "Matches docs/BENCHMARKS.md 1M ingest cadence (~1 WAL commit/s).",
        },
        "openpuffer": {
            "endpoint_shape": "POST /v2/namespaces/{namespace}",
            "first_batch_includes_schema": True,
            "ann_version_env": "OPENPUFFER_ANN_VERSION=3",
        },
        "turbopuffer": {
            "api": "namespace.write(upsert_columns=..., schema=...)",
            "batch_bytes_hint": "10k rows × 128 f32 is well under 512 MiB",
        },
        "generator": {
            "script": "benchmarks/workloads/generate_synthetic.py",
            "compatible_with": [
                "tests/bench_cold.rs synthetic_embedding",
                "tests/integration_s3.rs stress_upsert_columns",
                "src/index/vector.rs bench_synthetic_embedding",
            ],
        },
    }


def _vector_query_specs(cfg: WorkloadConfig, count: int) -> list[dict[str, Any]]:
    if count <= 0:
        return []
    step = max(1, cfg.num_docs // count)
    specs: list[dict[str, Any]] = []
    for q in range(count):
        doc_index = min(q * step, cfg.num_docs - 1)
        specs.append(
            {
                "name": f"vector-q{q:02d}",
                "doc_index": doc_index,
                "vector": embedding_for_doc(cfg, doc_index),
                "openpuffer_query": {
                    "rank_by": ["vector", "ANN", "embedding", "$vector"],
                    "top_k": 10,
                    "consistency": "strong",
                },
            }
        )
    return specs


def _filter_query_specs(cfg: WorkloadConfig, count: int) -> list[dict[str, Any]]:
    templates: list[dict[str, Any]] = [
        {
            "name": "filter-category-in-012",
            "filters": ["category", "In", ["cat-0", "cat-1", "cat-2"]],
            "openpuffer_query": {
                "rank_by": ["vector", "ANN", "embedding", "$vector"],
                "filters": ["category", "In", ["cat-0", "cat-1", "cat-2"]],
                "top_k": 10,
                "consistency": "strong",
            },
        },
        {
            "name": "filter-priority-gt-50",
            "filters": ["priority", "Gt", 50],
            "openpuffer_query": {
                "rank_by": ["vector", "ANN", "embedding", "$vector"],
                "filters": ["priority", "Gt", 50],
                "top_k": 10,
                "consistency": "strong",
            },
        },
        {
            "name": "filter-category-eq-cat-3",
            "filters": ["category", "Eq", "cat-3"],
            "openpuffer_query": {
                "rank_by": ["vector", "ANN", "embedding", "$vector"],
                "filters": ["category", "Eq", "cat-3"],
                "top_k": 10,
                "consistency": "strong",
            },
        },
        {
            "name": "filter-priority-lte-10",
            "filters": ["priority", "Lte", 10],
            "openpuffer_query": {
                "rank_by": ["vector", "ANN", "embedding", "$vector"],
                "filters": ["priority", "Lte", 10],
                "top_k": 10,
                "consistency": "strong",
            },
        },
        {
            "name": "filter-category-not-in-67",
            "filters": ["category", "NotIn", ["cat-6", "cat-7"]],
            "openpuffer_query": {
                "rank_by": ["vector", "ANN", "embedding", "$vector"],
                "filters": ["category", "NotIn", ["cat-6", "cat-7"]],
                "top_k": 10,
                "consistency": "strong",
            },
        },
        {
            "name": "filter-priority-between",
            "filters": ["And", [["priority", "Gte", 20], ["priority", "Lte", 30]]],
            "openpuffer_query": {
                "rank_by": ["vector", "ANN", "embedding", "$vector"],
                "filters": ["And", [["priority", "Gte", 20], ["priority", "Lte", 30]]],
                "top_k": 10,
                "consistency": "strong",
            },
        },
    ]
    out: list[dict[str, Any]] = []
    ref_index = min(1000, cfg.num_docs - 1)
    ref_vector = embedding_for_doc(cfg, ref_index)
    for t in templates[:count]:
        entry = dict(t)
        entry["reference_doc_index"] = ref_index
        entry["vector"] = ref_vector
        out.append(entry)
    return out


def _hybrid_query_specs(cfg: WorkloadConfig, count: int) -> list[dict[str, Any]]:
    templates: list[dict[str, Any]] = [
        {
            "name": "hybrid-sum-vector-bm25",
            "bm25_term": "stressterm",
            "openpuffer_query": {
                "rank_by": [
                    "Sum",
                    ["vector", "ANN", "embedding", "$vector"],
                    ["BM25", "text", "stressterm"],
                ],
                "top_k": 10,
                "consistency": "strong",
            },
        },
        {
            "name": "hybrid-sum-with-category-filter",
            "bm25_term": "document",
            "filters": ["category", "Eq", "cat-1"],
            "openpuffer_query": {
                "rank_by": [
                    "Sum",
                    ["vector", "ANN", "embedding", "$vector"],
                    ["BM25", "text", "document"],
                ],
                "filters": ["category", "Eq", "cat-1"],
                "top_k": 10,
                "consistency": "strong",
            },
        },
        {
            "name": "hybrid-product-vector-bm25",
            "bm25_term": "number",
            "openpuffer_query": {
                "rank_by": [
                    "Product",
                    ["vector", "ANN", "embedding", "$vector"],
                    ["BM25", "text", "number"],
                ],
                "top_k": 10,
                "consistency": "strong",
            },
        },
        {
            "name": "hybrid-sum-priority-filter",
            "bm25_term": "synthetic",
            "filters": ["priority", "Lt", 25],
            "openpuffer_query": {
                "rank_by": [
                    "Sum",
                    ["vector", "ANN", "embedding", "$vector"],
                    ["BM25", "text", "synthetic"],
                ],
                "filters": ["priority", "Lt", 25],
                "top_k": 10,
                "consistency": "strong",
            },
        },
    ]
    out: list[dict[str, Any]] = []
    for i, t in enumerate(templates[:count]):
        doc_index = min((i + 1) * 500, cfg.num_docs - 1)
        entry = dict(t)
        entry["doc_index"] = doc_index
        entry["vector"] = embedding_for_doc(cfg, doc_index)
        out.append(entry)
    return out


def spot_check_dict(*, count: int = DEFAULT_SPOT_CHECK_COUNT, top_k: int = 10) -> dict[str, Any]:
    """Phase 3.3 — first N pure vector ANN queries for cross-engine id overlap."""
    return {
        "count": count,
        "top_k": top_k,
        "include_attributes": True,
        "consistency": "strong",
        "source": "vector_queries",
        "notes": (
            "Compare id overlap@k between openpuffer and turbopuffer on the same query "
            "vectors and cosine_distance ANN. Different ANN graphs/probes may reduce "
            "overlap below k; record intersection@k for the report, not exact rank parity."
        ),
    }


def queries_dict(
    cfg: WorkloadConfig,
    *,
    num_vector: int = DEFAULT_NUM_VECTOR_QUERIES,
    num_filter: int = DEFAULT_NUM_FILTER_QUERIES,
    num_hybrid: int = DEFAULT_NUM_HYBRID_QUERIES,
    spot_check_count: int = DEFAULT_SPOT_CHECK_COUNT,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "seed": cfg.seed,
        "num_docs": cfg.num_docs,
        "dim": cfg.dim,
        "embedding_fn": cfg.embedding_fn,
        "vector_queries": _vector_query_specs(cfg, num_vector),
        "filter_queries": _filter_query_specs(cfg, num_filter),
        "hybrid_queries": _hybrid_query_specs(cfg, num_hybrid),
        "recall_defaults": {"num": 20, "top_k": 10, "vector_field": "embedding"},
        "cold_query_protocol": {
            "top_k": 10,
            "consistency": "strong",
            "runs": 7,
            "primary_query": "vector_queries[0]",
        },
        "warm_query_protocol": {
            "top_k": 10,
            "consistency": "eventual",
            "runs": 20,
            "primary_query": "vector_queries[0]",
        },
        "spot_check": spot_check_dict(count=spot_check_count),
    }


def iter_batches(cfg: WorkloadConfig) -> Iterator[tuple[int, int, int]]:
    """Yield (batch_index, start, count)."""
    for batch_index, start in enumerate(range(0, cfg.num_docs, cfg.batch_size)):
        count = min(cfg.batch_size, cfg.num_docs - start)
        yield batch_index, start, count


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def generate_workload(
    output_dir: Path,
    cfg: WorkloadConfig,
    *,
    write_batches: bool = False,
    batch_format: str = "openpuffer",
    num_vector_queries: int = DEFAULT_NUM_VECTOR_QUERIES,
    num_filter_queries: int = DEFAULT_NUM_FILTER_QUERIES,
    num_hybrid_queries: int = DEFAULT_NUM_HYBRID_QUERIES,
) -> None:
    write_json(output_dir / "manifest.json", manifest_dict(cfg))
    write_json(
        output_dir / "queries.json",
        queries_dict(
            cfg,
            num_vector=num_vector_queries,
            num_filter=num_filter_queries,
            num_hybrid=num_hybrid_queries,
        ),
    )

    if not write_batches:
        return

    batch_dir = output_dir / "batches"
    batch_dir.mkdir(parents=True, exist_ok=True)
    for batch_index, start, count in iter_batches(cfg):
        include_schema = batch_index == 0
        if batch_format == "openpuffer":
            body = openpuffer_write_body(cfg, start, count, include_schema=include_schema)
        elif batch_format == "turbopuffer":
            body = turbopuffer_write_kwargs(cfg, start, count, include_schema=include_schema)
        else:
            raise ValueError(f"unknown batch_format: {batch_format}")
        name = f"batch-{batch_index:05d}.json"
        write_json(batch_dir / name, body)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("benchmarks/workloads/synthetic-128/l1-100k"),
        help="Directory for manifest.json and queries.json",
    )
    p.add_argument("--num-docs", type=int, default=100_000, help="Total documents (L1 default 100k)")
    p.add_argument("--dim", type=int, default=DEFAULT_DIM)
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    p.add_argument(
        "--id-scheme",
        choices=("doc-prefix", "u64"),
        default="doc-prefix",
        help="doc-prefix → doc-{i} (matches bench_cold.rs)",
    )
    p.add_argument(
        "--embedding-fn",
        choices=("bench_sin_v1", "xorshift_f32"),
        default="bench_sin_v1",
        help="bench_sin_v1 matches existing gates; xorshift_f32 uses --seed per doc",
    )
    p.add_argument("--write-batches", action="store_true", help="Emit batch-*.json under output-dir/batches/")
    p.add_argument(
        "--batch-format",
        choices=("openpuffer", "turbopuffer"),
        default="openpuffer",
    )
    p.add_argument("--num-vector-queries", type=int, default=DEFAULT_NUM_VECTOR_QUERIES)
    p.add_argument("--num-filter-queries", type=int, default=DEFAULT_NUM_FILTER_QUERIES)
    p.add_argument("--num-hybrid-queries", type=int, default=DEFAULT_NUM_HYBRID_QUERIES)
    p.add_argument(
        "--verify-sample",
        action="store_true",
        help="Print first doc embedding sample to stdout and exit (no files written)",
    )
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = WorkloadConfig(
        seed=args.seed,
        num_docs=args.num_docs,
        dim=args.dim,
        batch_size=args.batch_size,
        id_scheme=args.id_scheme,
        embedding_fn=args.embedding_fn,
    )

    if args.verify_sample:
        sample = embedding_for_doc(cfg, 0)
        print(json.dumps({"doc-0": sample[:4], "len": len(sample)}))
        return 0

    generate_workload(
        args.output_dir,
        cfg,
        write_batches=args.write_batches,
        batch_format=args.batch_format,
        num_vector_queries=args.num_vector_queries,
        num_filter_queries=args.num_filter_queries,
        num_hybrid_queries=args.num_hybrid_queries,
    )
    print(f"wrote {args.output_dir}/manifest.json", file=sys.stderr)
    print(f"wrote {args.output_dir}/queries.json", file=sys.stderr)
    if args.write_batches:
        batches = (cfg.num_docs + cfg.batch_size - 1) // cfg.batch_size
        print(f"wrote {batches} batch file(s) under {args.output_dir}/batches/", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())