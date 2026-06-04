"""
Phase 3.3 — cross-engine id overlap spot-check (pure vector ANN).

Shared helpers for benchmarks/cross_check/run_spotcheck.py and pytest (offline/mock).
"""

from __future__ import annotations

import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

_REPORT_DIR = Path(__file__).resolve().parents[1] / "report"
if str(_REPORT_DIR) not in sys.path:
    sys.path.insert(0, str(_REPORT_DIR))

from schema_version import large_benchmark_json_schema_version  # noqa: E402
from utc_timestamps import benchmark_run_timestamps  # noqa: E402

DEFAULT_SPOT_CHECK_COUNT = 10

# Fallback when manifest.json is unavailable (synthetic-128 tiers).
TIER_EXPECTED_DOCS: dict[str, int] = {
    "l1": 100_000,
    "l2": 500_000,
    "l3": 1_000_000,
}

# Deterministic intersection sizes for offline mock (10 queries, top_k=10).
MOCK_INTERSECTION_COUNTS: tuple[int, ...] = (8, 7, 6, 7, 5, 8, 6, 7, 9, 6)


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def expected_docs_for_tier(tier: str, manifest: dict[str, Any] | None = None) -> int:
    """Expected indexed doc count for a synthetic-128 tier (from manifest or table)."""
    if manifest is not None and manifest.get("num_docs") is not None:
        return int(manifest["num_docs"])
    if tier not in TIER_EXPECTED_DOCS:
        raise ValueError(f"unknown tier for expected docs: {tier}")
    return TIER_EXPECTED_DOCS[tier]


def openpuffer_namespace_issues(
    meta: dict[str, Any] | None,
    *,
    expected_docs: int,
    namespace: str,
) -> list[str]:
    """Return human-readable blockers for live id-overlap (empty [] = ready)."""
    if meta is None:
        return [
            f"namespace {namespace!r} not found (GET /v1/namespaces returned no body)",
        ]
    wal_commit = int(meta.get("wal_commit_seq") or 0)
    index_cursor = int(meta.get("index_cursor") or 0)
    pref_ann = int(meta.get("preferred_ann_version") or 2)
    approx_rows = int(meta.get("approx_row_count") or 0)

    issues: list[str] = []
    if wal_commit == 0:
        issues.append(
            "wal_commit_seq=0 (namespace empty — run ./scripts/ingest-large.sh or "
            "./scripts/run-aws-large-benchmark.sh for this tier first)"
        )
    elif index_cursor != wal_commit:
        issues.append(
            f"index not caught up (index_cursor={index_cursor} != wal_commit_seq={wal_commit}; "
            "wait for ingest index poll or ./scripts/diagnose-index-lag.sh)"
        )
    if pref_ann != 3:
        issues.append(
            f"preferred_ann_version={pref_ann} (expected 3; re-ingest with OPENPUFFER_ANN_VERSION=3)"
        )
    if approx_rows == 0 and wal_commit > 0:
        issues.append(
            "approx_row_count=0 while wal_commit_seq>0 (metadata lag or corrupt namespace)"
        )
    elif 0 < approx_rows < expected_docs:
        issues.append(
            f"approx_row_count={approx_rows} < tier manifest num_docs={expected_docs} "
            "(partial ingest — finish ingest-large before overlap spot-check)"
        )
    return issues


def turbopuffer_namespace_issues(
    meta: Any,
    *,
    expected_docs: int,
    namespace: str,
) -> list[str]:
    """Return human-readable blockers for tpuf namespace (empty [] = ready)."""
    if meta is None:
        return [f"namespace {namespace!r} metadata unavailable"]

    index = getattr(meta, "index", None)
    status = getattr(index, "status", None) if index is not None else None
    row_count = getattr(meta, "approx_row_count", None)
    row_n = int(row_count) if row_count is not None else None

    issues: list[str] = []
    if row_n is None:
        issues.append(
            "approx_row_count missing (namespace may not exist — run "
            "./scripts/run-tpuf-large-benchmark.sh or tpuf_driver/run_benchmark.py ingest first)"
        )
    elif row_n == 0:
        issues.append(
            "approx_row_count=0 (namespace empty — run G4 ingest for this tier; "
            "check TURBOPUFFER_BENCH_NAMESPACE matches the ingested namespace)"
        )
    elif row_n < expected_docs:
        issues.append(
            f"approx_row_count={row_n} < tier manifest num_docs={expected_docs} "
            "(partial ingest — finish tpuf ingest before overlap spot-check)"
        )
    if status != "up-to-date":
        issues.append(
            f"index.status={status!r} (expected 'up-to-date'; wait for tpuf index gate after ingest)"
        )
    return issues


def format_namespace_preflight_error(
    *,
    engine: str,
    namespace: str,
    issues: list[str],
    tier: str,
    extra_hints: list[str] | None = None,
) -> str:
    """Single stderr/exit message for empty or unready namespaces."""
    lines = [
        f"id-overlap preflight: {engine} namespace not ready",
        f"  namespace: {namespace}",
        f"  tier: {tier}",
    ]
    for issue in issues:
        lines.append(f"  - {issue}")
    hints = list(extra_hints or [])
    if engine == "openpuffer":
        hints.extend(
            [
                "export OPENPUFFER_BASE_URL=http://127.0.0.1:8080",
                f"export OPENPUFFER_BENCH_NAMESPACE=bench-large-{tier}  # or your G3 namespace",
                f"curl -s \"$OPENPUFFER_BASE_URL/v1/namespaces/$OPENPUFFER_BENCH_NAMESPACE\" | jq",
            ]
        )
    else:
        hints.extend(
            [
                "export TURBOPUFFER_API_KEY=tpuf_...",
                f"export TURBOPUFFER_BENCH_NAMESPACE=bench-tpuf-$(date +%F)-{tier}",
                "./scripts/preflight-tpuf.sh --tier " + tier,
            ]
        )
    lines.append("Hints:")
    for hint in hints:
        lines.append(f"  {hint}")
    return "\n".join(lines)


def spot_check_config(queries: dict[str, Any]) -> dict[str, Any]:
    return queries.get("spot_check") or {
        "count": DEFAULT_SPOT_CHECK_COUNT,
        "top_k": 10,
        "include_attributes": True,
        "consistency": "strong",
        "source": "vector_queries",
    }


def spot_check_query_specs(queries: dict[str, Any]) -> list[dict[str, Any]]:
    """First N vector_queries per queries.json spot_check.count (default 10)."""
    cfg = spot_check_config(queries)
    count = int(cfg.get("count", DEFAULT_SPOT_CHECK_COUNT))
    vector_queries = queries.get("vector_queries") or []
    if not vector_queries:
        raise ValueError("queries.json has no vector_queries")
    return list(vector_queries[:count])


def substitute_vector(template: Any, vector: list[float]) -> Any:
    """Replace \"$vector\" placeholders (same contract as tests/common/synthetic_workload.rs)."""
    if isinstance(template, str) and template == "$vector":
        return vector
    if isinstance(template, list):
        return [substitute_vector(v, vector) for v in template]
    if isinstance(template, dict):
        return {k: substitute_vector(v, vector) for k, v in template.items()}
    return template


def openpuffer_query_body(
    spec: dict[str, Any],
    *,
    top_k: int,
    include_attributes: bool,
    consistency: str | None = None,
) -> dict[str, Any]:
    vector = list(spec["vector"])
    template = spec.get("openpuffer_query") or {
        "rank_by": ["vector", "ANN", "embedding", "$vector"],
        "top_k": top_k,
        "consistency": consistency or "strong",
    }
    body = substitute_vector(deepcopy(template), vector)
    body["top_k"] = top_k
    if consistency:
        body["consistency"] = consistency
    if include_attributes:
        body["include_attributes"] = True
    return body


def turbopuffer_query_kwargs(
    vector: list[float],
    *,
    top_k: int,
    consistency: str,
    include_attributes: bool,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "rank_by": ("vector", "ANN", "embedding", vector),
        "top_k": top_k,
        "consistency": consistency,
    }
    if include_attributes:
        kwargs["include_attributes"] = True
    return kwargs


def row_id(row: Any) -> str:
    if isinstance(row, dict):
        rid = row.get("id")
        return str(rid) if rid is not None else ""
    rid = getattr(row, "id", None)
    return str(rid) if rid is not None else ""


def extract_ids_openpuffer_response(resp: dict[str, Any]) -> list[str]:
    rows = resp.get("rows") or []
    return [str(r["id"]) for r in rows if r.get("id") is not None]


def extract_ids_tpuf_response(resp: Any) -> list[str]:
    rows = getattr(resp, "rows", None) or []
    return [row_id(r) for r in rows if row_id(r)]


def overlap_metrics(ids_a: list[str], ids_b: list[str], *, top_k: int) -> dict[str, Any]:
    """Intersection@k metrics for two ranked top-k id lists (order ignored)."""
    set_a = set(ids_a[:top_k])
    set_b = set(ids_b[:top_k])
    intersection = sorted(set_a & set_b)
    union = set_a | set_b
    k = max(top_k, 1)
    inter_n = len(intersection)
    union_n = len(union)
    jaccard = (inter_n / union_n) if union_n else 0.0
    overlap_at_k = inter_n / k
    return {
        "top_k": top_k,
        "openpuffer_count": len(ids_a),
        "turbopuffer_count": len(ids_b),
        "intersection_count": inter_n,
        "union_count": union_n,
        "overlap_at_k": round(overlap_at_k, 4),
        "jaccard": round(jaccard, 4),
        "intersection_ids": intersection,
        "openpuffer_ids": ids_a[:top_k],
        "turbopuffer_ids": ids_b[:top_k],
    }


def summarize_query_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    overlaps = [float(r["overlap_at_k"]) for r in results if "overlap_at_k" in r]
    if not overlaps:
        return {
            "query_count": 0,
            "mean_overlap_at_k": None,
            "min_overlap_at_k": None,
            "max_overlap_at_k": None,
        }
    return {
        "query_count": len(overlaps),
        "mean_overlap_at_k": round(sum(overlaps) / len(overlaps), 4),
        "min_overlap_at_k": round(min(overlaps), 4),
        "max_overlap_at_k": round(max(overlaps), 4),
    }


def synthetic_mock_id_lists(
    doc_index: int, *, top_k: int, intersection_count: int
) -> tuple[list[str], list[str]]:
    """Build ranked id lists with a fixed intersection size (offline mock only)."""
    op_ids = [f"doc-{doc_index + i}" for i in range(top_k)]
    inter = min(max(intersection_count, 0), top_k)
    shared = op_ids[:inter]
    tpuf_extra = [f"tpuf-{doc_index + top_k + i}" for i in range(top_k - inter)]
    tpuf_ids = shared + tpuf_extra
    return op_ids, tpuf_ids


def mock_intersection_counts(query_count: int) -> tuple[int, ...]:
    """Repeat MOCK_INTERSECTION_COUNTS to match spot_check query count."""
    base = MOCK_INTERSECTION_COUNTS
    if query_count <= len(base):
        return base[:query_count]
    out: list[int] = []
    while len(out) < query_count:
        out.extend(base)
    return tuple(out[:query_count])


def build_mock_payload(
    *,
    tier: str,
    workload_dir: str,
    queries: dict[str, Any],
    notes: str | None = None,
) -> dict[str, Any]:
    """Production-shaped mock payload from workload queries.json (no network)."""
    from datetime import date

    spot_cfg = spot_check_config(queries)
    top_k = int(spot_cfg.get("top_k", 10))
    specs = spot_check_query_specs(queries)
    per_query: list[dict[str, Any]] = []
    for spec, inter_n in zip(specs, mock_intersection_counts(len(specs))):
        doc_index = int(spec.get("doc_index", 0))
        op_ids, tpuf_ids = synthetic_mock_id_lists(
            doc_index, top_k=top_k, intersection_count=inter_n
        )
        metrics = overlap_metrics(op_ids, tpuf_ids, top_k=top_k)
        per_query.append(
            {
                "name": str(spec.get("name", "unknown")),
                "doc_index": spec.get("doc_index"),
                **metrics,
            }
        )
    return build_result_payload(
        tier=tier,
        workload_dir=workload_dir,
        spot_cfg=spot_cfg,
        per_query=per_query,
        mode="mock",
        openpuffer_namespace=f"bench-large-{tier}-mock",
        turbopuffer_namespace=f"bench-tpuf-{date.today().isoformat()}-{tier}-mock",
        notes=notes
        or (
            "Offline mock for render-report and pytest; "
            "not measured against live engines."
        ),
    )


def build_result_payload(
    *,
    tier: str,
    workload_dir: str,
    spot_cfg: dict[str, Any],
    per_query: list[dict[str, Any]],
    mode: str,
    openpuffer_namespace: str | None = None,
    turbopuffer_namespace: str | None = None,
    notes: str | None = None,
    started_at: str | None = None,
) -> dict[str, Any]:
    summary = summarize_query_results(per_query)
    return {
        "schema_version": large_benchmark_json_schema_version(),
        **benchmark_run_timestamps(started_at=started_at),
        "benchmark": "id_overlap_spotcheck",
        "tier": tier,
        "workload_dir": workload_dir,
        "mode": mode,
        "spot_check": spot_cfg,
        "openpuffer_namespace": openpuffer_namespace,
        "turbopuffer_namespace": turbopuffer_namespace,
        "queries": per_query,
        "summary": summary,
        "notes": notes
        or spot_cfg.get("notes")
        or (
            "Pure vector ANN overlap@k; expect divergence from different ANN graphs. "
            "Not a hard CI gate on live overlap."
        ),
    }