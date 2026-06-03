"""
Phase 3.3 — cross-engine id overlap spot-check (pure vector ANN).

Shared helpers for benchmarks/cross_check/run_spotcheck.py and pytest (offline/mock).
"""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

DEFAULT_SPOT_CHECK_COUNT = 10


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


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


def build_result_payload(
    *,
    tier: str,
    workload_dir: str,
    spot_cfg: dict[str, Any],
    per_query: list[dict[str, Any]],
    mode: str,
    openpuffer_namespace: str | None = None,
    turbopuffer_namespace: str | None = None,
) -> dict[str, Any]:
    summary = summarize_query_results(per_query)
    return {
        "benchmark": "id_overlap_spotcheck",
        "tier": tier,
        "workload_dir": workload_dir,
        "mode": mode,
        "spot_check": spot_cfg,
        "openpuffer_namespace": openpuffer_namespace,
        "turbopuffer_namespace": turbopuffer_namespace,
        "queries": per_query,
        "summary": summary,
        "notes": spot_cfg.get("notes")
        or (
            "Pure vector ANN overlap@k; expect divergence from different ANN graphs. "
            "Not a hard CI gate on live overlap."
        ),
    }