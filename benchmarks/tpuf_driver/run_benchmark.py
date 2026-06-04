#!/usr/bin/env python3
"""
Turbopuffer comparison harness (Phase 1 / A4).

Ingests the shared synthetic-128 workload and runs the same cold-query protocol as
scripts/bench-large.sh, writing benchmarks/results/tpuf-{tier}.json for render-report.

Requires TURBOPUFFER_API_KEY and a region aligned with the openpuffer AWS bench host.
See benchmarks/tpuf_driver/README.md.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
WORKLOADS_DIR = ROOT / "benchmarks" / "workloads"
sys.path.insert(0, str(WORKLOADS_DIR))

import generate_synthetic as gen  # noqa: E402

DEFAULT_REGION = "aws-us-east-1"
TIER_DEFAULTS: dict[str, tuple[int, str]] = {
    "l1": (100_000, "benchmarks/workloads/synthetic-128/l1-100k"),
    "l2": (500_000, "benchmarks/workloads/synthetic-128/l2-500k"),
    "l3": (1_000_000, "benchmarks/workloads/synthetic-128/l3-1m"),
}
TIER_INDEX_TIMEOUT_SEC: dict[str, int] = {
    "l1": 7200,
    "l2": 10800,
    "l3": 14400,
}
RECALL_GATE = 0.85


def default_index_timeout_sec(tier: str) -> int:
    raw = os.environ.get("TURBOPUFFER_BENCH_INDEX_TIMEOUT_SEC")
    if raw:
        return int(raw)
    return TIER_INDEX_TIMEOUT_SEC.get(tier, 7200)


@dataclass(frozen=True)
class RunContext:
    tier: str
    workload_dir: Path
    results_path: Path
    region: str
    namespace: str
    num_docs: int
    dim: int
    seed: int
    embedding_fn: str
    batch_size: int
    cold_runs: int
    query_top_k: int
    query_consistency: str
    primary_query_name: str
    query_vector: list[float]
    recall_num: int
    recall_top_k: int
    index_timeout_sec: int
    enforce_gates: bool
    skip_ingest: bool
    skip_delete: bool
    delete_first: bool
    warm_mode: bool
    warm_runs: int
    warm_query_top_k: int
    warm_consistency: str
    filter_specs: tuple[dict[str, Any], ...]
    hybrid_specs: tuple[dict[str, Any], ...]


def repo_relative(path: str) -> Path:
    return ROOT / path


def resolve_tier(tier: str) -> tuple[int, Path]:
    if tier not in TIER_DEFAULTS:
        raise SystemExit(f"unknown tier: {tier} (use l1, l2, or l3)")
    docs, rel = TIER_DEFAULTS[tier]
    return docs, repo_relative(rel)


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def workload_config_from_manifest(manifest: dict[str, Any], num_docs: int) -> gen.WorkloadConfig:
    return gen.WorkloadConfig(
        seed=int(manifest.get("seed", gen.DEFAULT_SEED)),
        num_docs=num_docs,
        dim=int(manifest.get("dim", gen.DEFAULT_DIM)),
        batch_size=int(manifest.get("batch_size", gen.DEFAULT_BATCH_SIZE)),
        id_scheme=str(manifest.get("id_scheme", "doc-prefix")),
        embedding_fn=str(manifest.get("embedding_fn", "bench_sin_v1")),
        distance_metric=str(manifest.get("distance_metric", "cosine_distance")),
    )


def default_namespace(tier: str) -> str:
    stamp = date.today().isoformat()
    return f"bench-tpuf-{stamp}-{tier}"


def percentile_ms(sorted_latencies: list[int], pct: int) -> int:
    n = len(sorted_latencies)
    if n == 0:
        return 0
    idx = (n * pct + 99) // 100 - 1
    return sorted_latencies[max(0, min(idx, n - 1))]


def index_is_ready(meta: Any, expected_docs: int) -> tuple[bool, str, int | None]:
    """Return (ready, status, approx_row_count)."""
    index = getattr(meta, "index", None)
    status = getattr(index, "status", None) if index is not None else None
    row_count = getattr(meta, "approx_row_count", None)
    if status != "up-to-date":
        return False, str(status or "unknown"), row_count
    if row_count is not None and int(row_count) < expected_docs:
        return False, status, int(row_count)
    return True, status, int(row_count) if row_count is not None else None


def wait_until_indexed(ns: Any, *, expected_docs: int, timeout_sec: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_sec
    last: dict[str, Any] = {"status": "unknown", "approx_row_count": None}
    while time.monotonic() < deadline:
        meta = ns.metadata()
        ready, status, row_count = index_is_ready(meta, expected_docs)
        last = {"status": status, "approx_row_count": row_count}
        if ready:
            return last
        time.sleep(2.0)
    raise TimeoutError(
        f"timeout waiting for index up-to-date with >= {expected_docs} rows "
        f"(last status={last['status']}, approx_row_count={last['approx_row_count']})"
    )


def ingest_workload(ns: Any, cfg: gen.WorkloadConfig) -> dict[str, Any]:
    t0 = time.monotonic()
    total_rows = 0
    batches: list[dict[str, Any]] = []
    for batch_index, start, count in gen.iter_batches(cfg):
        kwargs = gen.turbopuffer_write_kwargs(
            cfg, start, count, include_schema=batch_index == 0
        )
        batch_t0 = time.monotonic()
        resp = ns.write(**kwargs)
        batch_ms = int((time.monotonic() - batch_t0) * 1000)
        affected = int(getattr(resp, "rows_affected", count) or count)
        total_rows += affected
        batches.append(
            {
                "batch_index": batch_index,
                "start": start,
                "count": count,
                "rows_affected": affected,
                "wall_ms": batch_ms,
            }
        )
    elapsed = time.monotonic() - t0
    return {
        "ingest_elapsed_secs": round(elapsed, 2),
        "ingest_docs_per_sec": round(total_rows / elapsed, 2) if elapsed > 0 else 0.0,
        "ingest_rows_written": total_rows,
        "ingest_batches": batches,
    }


def inject_vector_placeholder(value: Any, vector: list[float]) -> Any:
    """Substitute ``\"$vector\"`` placeholders (same contract as synthetic_workload.rs)."""
    if value == "$vector":
        return vector
    if isinstance(value, list):
        return [inject_vector_placeholder(v, vector) for v in value]
    if isinstance(value, dict):
        return {k: inject_vector_placeholder(v, vector) for k, v in value.items()}
    return value


def json_to_rank_by(value: Any, vector: list[float]) -> Any:
    """Convert openpuffer ``rank_by`` JSON lists to turbopuffer tuple form."""
    resolved = inject_vector_placeholder(value, vector)
    if isinstance(resolved, list):
        return tuple(json_to_rank_by(v, vector) for v in resolved)
    return resolved


def openpuffer_query_kwargs(query: dict[str, Any], vector: list[float]) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "rank_by": json_to_rank_by(query["rank_by"], vector),
        "top_k": int(query.get("top_k", 10)),
        "consistency": str(query.get("consistency", "strong")),
        "include_attributes": False,
    }
    if query.get("filters") is not None:
        kwargs["filters"] = query["filters"]
    return kwargs


def performance_dict(perf: Any | None) -> dict[str, Any]:
    if perf is None:
        return {}
    if hasattr(perf, "model_dump"):
        return {k: v for k, v in perf.model_dump().items() if v is not None}
    if isinstance(perf, dict):
        return {k: v for k, v in perf.items() if v is not None}
    if hasattr(perf, "__dict__"):
        return {
            k: v
            for k, v in vars(perf).items()
            if v is not None and not k.startswith("_")
        }
    return {}


def query_once(ns: Any, **query_kwargs: Any) -> dict[str, Any]:
    t0 = time.perf_counter()
    resp = ns.query(**query_kwargs)
    latency_ms = int((time.perf_counter() - t0) * 1000)
    perf = performance_dict(getattr(resp, "performance", None))
    client_ms = perf.get("client_total_ms")
    if client_ms is not None:
        latency_ms = int(client_ms)
    approx_size = perf.get("approx_namespace_size")
    exhaustive = perf.get("exhaustive_search_count")
    ratio: float | None = None
    if approx_size and exhaustive is not None and approx_size > 0:
        ratio = float(exhaustive) / float(approx_size)
    return {
        "latency_ms": latency_ms,
        "performance": perf,
        "candidates_ratio": ratio,
        "result_rows": len(getattr(resp, "rows", []) or []),
    }


def cold_vector_query_kwargs(ctx: RunContext) -> dict[str, Any]:
    return {
        "rank_by": ("vector", "ANN", "embedding", ctx.query_vector),
        "top_k": ctx.query_top_k,
        "consistency": ctx.query_consistency,
        "include_attributes": False,
    }


def run_cold_queries(ctx: RunContext, ns: Any) -> tuple[list[dict[str, Any]], int, int, float | None]:
    runs: list[dict[str, Any]] = []
    latencies: list[int] = []
    last_ratio: float | None = None
    cold_kwargs = cold_vector_query_kwargs(ctx)
    for run_i in range(1, ctx.cold_runs + 1):
        sample = query_once(ns, **cold_kwargs)
        latencies.append(sample["latency_ms"])
        last_ratio = sample.get("candidates_ratio")
        runs.append(
            {
                "run": run_i,
                "query_name": ctx.primary_query_name,
                "latency_ms": sample["latency_ms"],
                "storage_roundtrips": None,
                "candidates_ratio": sample.get("candidates_ratio"),
                "cold_s3_keys_fetched": None,
                "tpuf_performance": sample.get("performance"),
            }
        )
    sorted_lat = sorted(latencies)
    return runs, percentile_ms(sorted_lat, 50), percentile_ms(sorted_lat, 95), last_ratio


def run_workload_query_specs(
    ns: Any,
    specs: tuple[dict[str, Any], ...],
    *,
    query_kind: str,
) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for spec in specs:
        name = str(spec.get("name", query_kind))
        vector = list(spec["vector"])
        oq = spec["openpuffer_query"]
        if not isinstance(oq, dict):
            raise ValueError(f"{query_kind} query {name}: openpuffer_query must be an object")
        sample = query_once(ns, **openpuffer_query_kwargs(oq, vector))
        runs.append(
            {
                "query_name": name,
                "query_kind": query_kind,
                "latency_ms": sample["latency_ms"],
                "result_rows": sample.get("result_rows"),
                "candidates_ratio": sample.get("candidates_ratio"),
                "tpuf_performance": sample.get("performance"),
            }
        )
    return runs


def run_warm_queries(ctx: RunContext, ns: Any) -> tuple[list[dict[str, Any]], int, int]:
    print(f"hint_cache_warm on namespace {ctx.namespace}…")
    ns.hint_cache_warm()
    warm_kwargs = {
        "rank_by": ("vector", "ANN", "embedding", ctx.query_vector),
        "top_k": ctx.warm_query_top_k,
        "consistency": ctx.warm_consistency,
        "include_attributes": False,
    }
    runs: list[dict[str, Any]] = []
    latencies: list[int] = []
    last_ratio: float | None = None
    for run_i in range(1, ctx.warm_runs + 1):
        sample = query_once(ns, **warm_kwargs)
        latencies.append(sample["latency_ms"])
        last_ratio = sample.get("candidates_ratio")
        runs.append(
            {
                "run": run_i,
                "query_name": ctx.primary_query_name,
                "latency_ms": sample["latency_ms"],
                "candidates_ratio": last_ratio,
                "tpuf_performance": sample.get("performance"),
            }
        )
    sorted_lat = sorted(latencies)
    return runs, percentile_ms(sorted_lat, 50), percentile_ms(sorted_lat, 95)


def run_recall(ns: Any, *, num: int, top_k: int) -> float:
    resp = ns.recall(num=num, top_k=top_k)
    return float(getattr(resp, "avg_recall", 0.0) or 0.0)


def build_context(args: argparse.Namespace) -> RunContext:
    tier = args.tier
    tier_docs, tier_workload = resolve_tier(tier)
    workload_dir = Path(args.workload_dir) if args.workload_dir else tier_workload
    if not workload_dir.is_absolute():
        workload_dir = ROOT / workload_dir

    manifest_path = workload_dir / "manifest.json"
    queries_path = workload_dir / "queries.json"
    manifest: dict[str, Any] = {}
    queries: dict[str, Any] = {}
    if manifest_path.is_file():
        manifest = load_json(manifest_path)
    if queries_path.is_file():
        queries = load_json(queries_path)

    num_docs = int(os.environ.get("TURBOPUFFER_BENCH_DOCS", manifest.get("num_docs", tier_docs)))
    dim = int(manifest.get("dim", gen.DEFAULT_DIM))
    seed = int(manifest.get("seed", gen.DEFAULT_SEED))
    embedding_fn = str(manifest.get("embedding_fn", "bench_sin_v1"))
    batch_size = int(manifest.get("batch_size", gen.DEFAULT_BATCH_SIZE))

    protocol = queries.get("cold_query_protocol", {})
    cold_runs = int(os.environ.get("TURBOPUFFER_BENCH_COLD_RUNS", protocol.get("runs", 7)))
    query_top_k = int(protocol.get("top_k", 10))
    query_consistency = str(protocol.get("consistency", "strong"))
    recall_defaults = queries.get("recall_defaults", {})
    recall_num = int(os.environ.get("TURBOPUFFER_BENCH_RECALL_NUM", recall_defaults.get("num", 20)))
    recall_top_k = int(
        os.environ.get("TURBOPUFFER_BENCH_RECALL_TOP_K", recall_defaults.get("top_k", 10))
    )

    vector_queries = queries.get("vector_queries") or []
    if vector_queries:
        primary = vector_queries[0]
        primary_query_name = str(primary.get("name", "vector-q00"))
        query_vector = list(primary["vector"])
    else:
        primary_query_name = "vector-q00-fallback"
        cfg = gen.WorkloadConfig(
            seed=seed,
            num_docs=num_docs,
            dim=dim,
            batch_size=batch_size,
            id_scheme="doc-prefix",
            embedding_fn=embedding_fn,
        )
        query_vector = gen.embedding_for_doc(cfg, 0)

    region = os.environ.get("TURBOPUFFER_REGION", DEFAULT_REGION)
    namespace = os.environ.get("TURBOPUFFER_BENCH_NAMESPACE", default_namespace(tier))
    results = os.environ.get(
        "TURBOPUFFER_BENCH_RESULTS",
        str(ROOT / "benchmarks" / "results" / f"tpuf-{tier}.json"),
    )
    results_path = Path(results)
    if not results_path.is_absolute():
        results_path = ROOT / results_path

    warm_protocol = queries.get("warm_query_protocol", {})
    warm_mode = bool(args.warm or os.environ.get("TURBOPUFFER_BENCH_WARM") == "1")
    warm_runs = int(os.environ.get("TURBOPUFFER_BENCH_WARM_RUNS", warm_protocol.get("runs", 20)))
    warm_query_top_k = int(warm_protocol.get("top_k", 10))
    warm_consistency = str(warm_protocol.get("consistency", "eventual"))

    filter_specs = tuple(queries.get("filter_queries") or ())
    hybrid_specs = tuple(queries.get("hybrid_queries") or ())

    index_timeout = default_index_timeout_sec(tier)
    enforce_gates = os.environ.get("TURBOPUFFER_BENCH_ENFORCE_GATES", "1") != "0"
    skip_ingest = bool(args.skip_ingest or os.environ.get("TURBOPUFFER_BENCH_SKIP_INGEST"))
    skip_delete = bool(args.skip_delete or os.environ.get("TURBOPUFFER_BENCH_SKIP_DELETE"))
    delete_first = os.environ.get("TURBOPUFFER_BENCH_DELETE_FIRST", "1") not in ("0", "false", "no")

    return RunContext(
        tier=tier,
        workload_dir=workload_dir,
        results_path=results_path,
        region=region,
        namespace=namespace,
        num_docs=num_docs,
        dim=dim,
        seed=seed,
        embedding_fn=embedding_fn,
        batch_size=batch_size,
        cold_runs=cold_runs,
        query_top_k=query_top_k,
        query_consistency=query_consistency,
        primary_query_name=primary_query_name,
        query_vector=query_vector,
        recall_num=recall_num,
        recall_top_k=recall_top_k,
        index_timeout_sec=index_timeout,
        enforce_gates=enforce_gates,
        skip_ingest=skip_ingest,
        skip_delete=skip_delete,
        delete_first=delete_first,
        warm_mode=warm_mode,
        warm_runs=warm_runs,
        warm_query_top_k=warm_query_top_k,
        warm_consistency=warm_consistency,
        filter_specs=filter_specs,
        hybrid_specs=hybrid_specs,
    )


def build_result_payload(
    ctx: RunContext,
    *,
    index_meta: dict[str, Any],
    ingest_stats: dict[str, Any] | None,
    cold_runs: list[dict[str, Any]],
    p50_ms: int,
    p95_ms: int,
    candidates_ratio: float | None,
    recall_at_10: float,
    filter_query_runs: list[dict[str, Any]] | None = None,
    hybrid_query_runs: list[dict[str, Any]] | None = None,
    warm_runs: list[dict[str, Any]] | None = None,
    warm_p50_ms: int | None = None,
    warm_p95_ms: int | None = None,
) -> dict[str, Any]:
    indexed = index_meta.get("status") == "up-to-date"
    warm_note = ""
    if ctx.warm_mode:
        warm_note = (
            f" Warm: hint_cache_warm + {ctx.warm_runs}× {ctx.primary_query_name} "
            f"(consistency={ctx.warm_consistency})."
        )
    secondary_note = ""
    if filter_query_runs or hybrid_query_runs:
        secondary_note = (
            f" Secondary: {len(filter_query_runs or [])} filter + "
            f"{len(hybrid_query_runs or [])} hybrid queries (1× each, strong)."
        )
    notes = (
        f"A4 run_benchmark.py tier={ctx.tier}; workload queries.json; "
        f"region={ctx.region}. Cold runs use consistency={ctx.query_consistency} on a "
        "fresh namespace (no openpuffer-style cache bust)."
        f"{warm_note}{secondary_note} "
        f"Targets: recall@10>={RECALL_GATE}. Regenerate: "
        f"python3 benchmarks/tpuf_driver/run_benchmark.py --tier {ctx.tier}"
        f"{' --warm' if ctx.warm_mode else ''}"
    )
    payload: dict[str, Any] = {
        "benchmark": f"cold_tpuf_{ctx.tier}",
        "environment": f"turbopuffer:{ctx.region}",
        "tier": ctx.tier,
        "workload_dir": str(ctx.workload_dir.relative_to(ROOT))
        if ctx.workload_dir.is_relative_to(ROOT)
        else str(ctx.workload_dir),
        "namespace": ctx.namespace,
        "primary_query": ctx.primary_query_name,
        "namespace_docs": ctx.num_docs,
        "dimensions": ctx.dim,
        "seed": ctx.seed,
        "embedding_fn": ctx.embedding_fn,
        "cache_dir": None,
        "consistency": ctx.query_consistency,
        "tpuf_region": ctx.region,
        "preferred_ann_version": None,
        "index_cursor_eq_wal_commit_seq": None,
        "index_up_to_date": indexed,
        "index_status": index_meta.get("status"),
        "approx_row_count": index_meta.get("approx_row_count"),
        "storage_roundtrips": None,
        "cold_s3_keys_fetched": None,
        "s3_get_count": None,
        "s3_get_count_note": "N/A (managed turbopuffer)",
        "p50_query_latency_ms": p50_ms,
        "p95_query_latency_ms": p95_ms,
        "candidates_ratio": candidates_ratio,
        "recall_at_10": recall_at_10,
        "cold_query_runs": ctx.cold_runs,
        "cold_runs": cold_runs,
        "index_keys_total": None,
        "index_object_count": None,
        "notes": notes,
    }
    if ingest_stats:
        payload.update(
            {
                "ingest_elapsed_secs": ingest_stats["ingest_elapsed_secs"],
                "ingest_docs_per_sec": ingest_stats["ingest_docs_per_sec"],
                "ingest_rows_written": ingest_stats["ingest_rows_written"],
            }
        )
    if filter_query_runs is not None:
        payload["filter_query_runs"] = filter_query_runs
    if hybrid_query_runs is not None:
        payload["hybrid_query_runs"] = hybrid_query_runs
    if ctx.warm_mode and warm_runs is not None:
        payload.update(
            {
                "p50_warm_query_latency_ms": warm_p50_ms,
                "p95_warm_query_latency_ms": warm_p95_ms,
                "warm_query_runs": ctx.warm_runs,
                "warm_consistency": ctx.warm_consistency,
                "warm_protocol": "hint_cache_warm",
                "warm_runs": warm_runs,
            }
        )
    return payload


def dry_run(ctx: RunContext) -> None:
    print("tpuf benchmark dry-run OK")
    print(f"  tier={ctx.tier} workload_dir={ctx.workload_dir}")
    print(f"  namespace={ctx.namespace} docs={ctx.num_docs} dim={ctx.dim}")
    print(f"  region={ctx.region} results={ctx.results_path}")
    print(f"  cold_runs={ctx.cold_runs} primary_query={ctx.primary_query_name}")
    print(f"  recall_num={ctx.recall_num} index_timeout={ctx.index_timeout_sec}s")
    print(
        f"  enforce_gates={ctx.enforce_gates} skip_ingest={ctx.skip_ingest} "
        f"delete_first={ctx.delete_first} skip_delete={ctx.skip_delete} warm_mode={ctx.warm_mode}"
    )
    print(f"  filter_queries={len(ctx.filter_specs)} hybrid_queries={len(ctx.hybrid_specs)}")
    if ctx.warm_mode:
        print(
            f"  warm_runs={ctx.warm_runs} warm_consistency={ctx.warm_consistency} "
            f"warm_top_k={ctx.warm_query_top_k} (hint_cache_warm)"
        )
    if os.environ.get("TURBOPUFFER_API_KEY"):
        print("  TURBOPUFFER_API_KEY=set")
    else:
        print("  TURBOPUFFER_API_KEY unset (required for full run)")
    warm_flag = " --warm" if ctx.warm_mode else ""
    print(
        f"Full run: export TURBOPUFFER_API_KEY=tpuf_... TURBOPUFFER_REGION={ctx.region} "
        f"&& python3 benchmarks/tpuf_driver/run_benchmark.py --tier {ctx.tier}{warm_flag}"
    )


def enforce_result_gates(ctx: RunContext, payload: dict[str, Any]) -> None:
    if not ctx.enforce_gates:
        return
    recall = float(payload.get("recall_at_10") or 0.0)
    indexed = payload.get("index_up_to_date") is True
    if not indexed or recall < RECALL_GATE:
        raise SystemExit(
            f"tpuf gates failed (need index_up_to_date and recall@10>={RECALL_GATE}). "
            "Set TURBOPUFFER_BENCH_ENFORCE_GATES=0 to record only."
        )


def run_live(ctx: RunContext) -> dict[str, Any]:
    api_key = os.environ.get("TURBOPUFFER_API_KEY")
    if not api_key:
        raise SystemExit(
            "set TURBOPUFFER_API_KEY (see https://turbopuffer.com/docs/testing)"
        )
    try:
        from turbopuffer import Turbopuffer
    except ImportError as exc:
        raise SystemExit(
            "install turbopuffer: pip install -r benchmarks/tpuf_driver/requirements.txt"
        ) from exc

    cfg = gen.WorkloadConfig(
        seed=ctx.seed,
        num_docs=ctx.num_docs,
        dim=ctx.dim,
        batch_size=ctx.batch_size,
        id_scheme="doc-prefix",
        embedding_fn=ctx.embedding_fn,
    )

    client = Turbopuffer(region=ctx.region, api_key=api_key)
    ns = client.namespace(ctx.namespace)
    ingest_stats: dict[str, Any] | None = None

    try:
        if not ctx.skip_ingest:
            if ctx.delete_first:
                print(f"DELETE_FIRST: clearing namespace {ctx.namespace} before ingest…")
                try:
                    ns.delete_all()
                except Exception as exc:  # noqa: BLE001 — namespace may not exist yet
                    print(f"  delete_all (pre-ingest): {exc}", file=sys.stderr)
            print(
                f"tpuf ingest: tier={ctx.tier} namespace={ctx.namespace} "
                f"docs={ctx.num_docs} region={ctx.region}"
            )
            ingest_stats = ingest_workload(ns, cfg)
            print(
                f"  ingest done: {ingest_stats['ingest_rows_written']} rows in "
                f"{ingest_stats['ingest_elapsed_secs']}s "
                f"({ingest_stats['ingest_docs_per_sec']} docs/s)"
            )
        else:
            print(f"Skipping ingest (namespace={ctx.namespace})")

        print(f"Waiting for index up-to-date (timeout {ctx.index_timeout_sec}s)…")
        index_meta = wait_until_indexed(
            ns, expected_docs=ctx.num_docs, timeout_sec=ctx.index_timeout_sec
        )
        print(f"  index ready: status={index_meta['status']} rows={index_meta['approx_row_count']}")

        print(f"Running {ctx.cold_runs} cold vector queries ({ctx.primary_query_name})…")
        cold_runs, p50, p95, ratio = run_cold_queries(ctx, ns)

        filter_query_runs: list[dict[str, Any]] | None = None
        hybrid_query_runs: list[dict[str, Any]] | None = None
        if ctx.filter_specs:
            print(f"Running {len(ctx.filter_specs)} filter queries (1× each)…")
            filter_query_runs = run_workload_query_specs(
                ns, ctx.filter_specs, query_kind="filter"
            )
        if ctx.hybrid_specs:
            print(f"Running {len(ctx.hybrid_specs)} hybrid queries (1× each)…")
            hybrid_query_runs = run_workload_query_specs(
                ns, ctx.hybrid_specs, query_kind="hybrid"
            )

        warm_runs_data: list[dict[str, Any]] | None = None
        warm_p50: int | None = None
        warm_p95: int | None = None
        if ctx.warm_mode:
            print(
                f"Warm phase: {ctx.warm_runs} queries ({ctx.primary_query_name}, "
                f"consistency={ctx.warm_consistency})…"
            )
            warm_runs_data, warm_p50, warm_p95 = run_warm_queries(ctx, ns)

        print(f"Measuring recall (num={ctx.recall_num}, top_k={ctx.recall_top_k})…")
        recall = run_recall(ns, num=ctx.recall_num, top_k=ctx.recall_top_k)

        payload = build_result_payload(
            ctx,
            index_meta=index_meta,
            ingest_stats=ingest_stats,
            cold_runs=cold_runs,
            p50_ms=p50,
            p95_ms=p95,
            candidates_ratio=ratio,
            recall_at_10=recall,
            filter_query_runs=filter_query_runs,
            hybrid_query_runs=hybrid_query_runs,
            warm_runs=warm_runs_data,
            warm_p50_ms=warm_p50,
            warm_p95_ms=warm_p95,
        )
        ctx.results_path.parent.mkdir(parents=True, exist_ok=True)
        with ctx.results_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
            f.write("\n")
        print(f"Wrote {ctx.results_path}")
        enforce_result_gates(ctx, payload)
        if ctx.enforce_gates:
            print(f"tpuf gates passed (tier={ctx.tier}).")
        return payload
    finally:
        if not ctx.skip_delete:
            print(f"Deleting namespace {ctx.namespace}…")
            try:
                ns.delete_all()
            except Exception as exc:  # noqa: BLE001 — best-effort cleanup
                print(f"warning: namespace delete failed: {exc}", file=sys.stderr)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest synthetic-128 workload on turbopuffer and emit tpuf-{tier}.json",
    )
    parser.add_argument("--tier", default=os.environ.get("TURBOPUFFER_BENCH_TIER", "l1"))
    parser.add_argument("--workload-dir", default=os.environ.get("TURBOPUFFER_BENCH_WORKLOAD_DIR"))
    parser.add_argument("--dry-run", "-n", action="store_true")
    parser.add_argument(
        "--warm",
        action="store_true",
        help="After cold/filter/hybrid: hint_cache_warm + warm_query_protocol runs",
    )
    parser.add_argument("--skip-ingest", action="store_true")
    parser.add_argument("--skip-delete", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if os.environ.get("TURBOPUFFER_BENCH_DRY_RUN") == "1":
        args.dry_run = True
    ctx = build_context(args)
    if args.dry_run:
        dry_run(ctx)
        return 0
    run_live(ctx)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())