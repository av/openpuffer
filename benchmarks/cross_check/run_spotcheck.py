#!/usr/bin/env python3
"""
Phase 3.3 — id overlap spot-check between openpuffer and turbopuffer.

For the first N vector queries in queries.json (default 10), run top_k=10 ANN with
include_attributes on both systems and record intersection@k.

Modes:
  --dry-run   Validate workload + list queries (no network)
  --mock      Use benchmarks/cross_check/fixtures/overlap-{tier}-mock.json
  (default)   Live queries when OPENPUFFER_BASE_URL + TURBOPUFFER_API_KEY are set

Output: benchmarks/results/id-overlap-{tier}.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks" / "cross_check"))
sys.path.insert(0, str(ROOT / "benchmarks" / "report"))

from utc_timestamps import utc_now_iso  # noqa: E402

import id_overlap as xcheck  # noqa: E402

TIER_WORKLOADS: dict[str, str] = {
    "l1": "benchmarks/workloads/synthetic-128/l1-100k",
    "l2": "benchmarks/workloads/synthetic-128/l2-500k",
    "l3": "benchmarks/workloads/synthetic-128/l3-1m",
}


def resolve_workload_dir(tier: str, override: str | None) -> Path:
    rel = override or TIER_WORKLOADS.get(tier)
    if not rel:
        raise SystemExit(f"unknown tier: {tier} (use l1, l2, or l3)")
    path = Path(rel)
    return path if path.is_absolute() else ROOT / path


def openpuffer_query(
    base_url: str,
    namespace: str,
    body: dict[str, Any],
    *,
    timeout_sec: int = 120,
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/v2/namespaces/{namespace}/query"
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run_mock(
    *,
    tier: str,
    workload_dir: Path,
    queries: dict[str, Any],
    fixture_path: Path | None,
) -> dict[str, Any]:
    rel_workload = (
        str(workload_dir.relative_to(ROOT))
        if workload_dir.is_relative_to(ROOT)
        else str(workload_dir)
    )
    if fixture_path is not None:
        payload = xcheck.load_json(fixture_path)
        if payload.get("tier") and payload["tier"] != tier:
            print(
                f"warning: fixture tier={payload['tier']} != --tier {tier}",
                file=sys.stderr,
            )
        return payload
    return xcheck.build_mock_payload(
        tier=tier,
        workload_dir=rel_workload,
        queries=queries,
    )


def run_live(
    *,
    tier: str,
    workload_dir: Path,
    queries: dict[str, Any],
    results_path: Path,
    timeout_sec: int,
) -> dict[str, Any]:
    spot_cfg = xcheck.spot_check_config(queries)
    top_k = int(spot_cfg.get("top_k", 10))
    include_attrs = bool(spot_cfg.get("include_attributes", True))
    consistency = str(spot_cfg.get("consistency", "strong"))

    op_base = os.environ.get("OPENPUFFER_BASE_URL", "http://127.0.0.1:8080")
    op_ns = os.environ.get(
        "OPENPUFFER_BENCH_NAMESPACE",
        os.environ.get("OPENPUFFER_NAMESPACE", f"bench-large-{tier}"),
    )
    tpuf_region = os.environ.get("TURBOPUFFER_REGION", "aws-us-east-1")
    tpuf_ns = os.environ.get(
        "TURBOPUFFER_BENCH_NAMESPACE",
        f"bench-tpuf-{date.today().isoformat()}-{tier}",
    )

    started_at = utc_now_iso()

    api_key = os.environ.get("TURBOPUFFER_API_KEY")
    if not api_key:
        raise SystemExit(
            "live spot-check requires TURBOPUFFER_API_KEY "
            "(or use --mock / --dry-run)"
        )

    try:
        from turbopuffer import Turbopuffer
    except ImportError as exc:
        raise SystemExit(
            "pip install -r benchmarks/requirements.txt"
        ) from exc

    client = Turbopuffer(api_key=api_key, region=tpuf_region)
    ns = client.namespace(tpuf_ns)

    per_query: list[dict[str, Any]] = []
    for spec in xcheck.spot_check_query_specs(queries):
        name = str(spec.get("name", "unknown"))
        vector = list(spec["vector"])
        op_body = xcheck.openpuffer_query_body(
            spec,
            top_k=top_k,
            include_attributes=include_attrs,
            consistency=consistency,
        )
        op_resp = openpuffer_query(op_base, op_ns, op_body, timeout_sec=timeout_sec)
        op_ids = xcheck.extract_ids_openpuffer_response(op_resp)

        tpuf_resp = ns.query(
            **xcheck.turbopuffer_query_kwargs(
                vector,
                top_k=top_k,
                consistency=consistency,
                include_attributes=include_attrs,
            )
        )
        tpuf_ids = xcheck.extract_ids_tpuf_response(tpuf_resp)
        metrics = xcheck.overlap_metrics(op_ids, tpuf_ids, top_k=top_k)
        per_query.append(
            {
                "name": name,
                "doc_index": spec.get("doc_index"),
                **metrics,
            }
        )

    rel_workload = (
        str(workload_dir.relative_to(ROOT))
        if workload_dir.is_relative_to(ROOT)
        else str(workload_dir)
    )
    return xcheck.build_result_payload(
        tier=tier,
        workload_dir=rel_workload,
        spot_cfg=spot_cfg,
        per_query=per_query,
        mode="live",
        openpuffer_namespace=op_ns,
        turbopuffer_namespace=tpuf_ns,
        started_at=started_at,
    )


def run_dry_run(*, tier: str, workload_dir: Path, queries: dict[str, Any]) -> int:
    spot_cfg = xcheck.spot_check_config(queries)
    specs = xcheck.spot_check_query_specs(queries)
    rel = (
        str(workload_dir.relative_to(ROOT))
        if workload_dir.is_relative_to(ROOT)
        else str(workload_dir)
    )
    print(f"id-overlap spot-check dry-run OK tier={tier} workload={rel}")
    print(f"  spot_check: count={spot_cfg.get('count')} top_k={spot_cfg.get('top_k')}")
    for spec in specs:
        print(f"  - {spec['name']} doc_index={spec.get('doc_index')}")
    print(
        "Live: export TURBOPUFFER_API_KEY=... OPENPUFFER_BASE_URL=http://127.0.0.1:8080 "
        f"&& python3 benchmarks/cross_check/run_spotcheck.py --tier {tier}"
    )
    print(f"Mock: python3 benchmarks/cross_check/run_spotcheck.py --tier {tier} --mock")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Phase 3.3 id overlap spot-check")
    parser.add_argument("--tier", choices=("l1", "l2", "l3"), default="l1")
    parser.add_argument("--workload-dir", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--mock", action="store_true")
    parser.add_argument(
        "--fixture",
        default=None,
        help="Mock JSON path (default benchmarks/cross_check/fixtures/overlap-{tier}-mock.json)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Default benchmarks/results/id-overlap-{tier}.json",
    )
    parser.add_argument("--timeout-sec", type=int, default=120)
    args = parser.parse_args(argv)

    workload_dir = resolve_workload_dir(args.tier, args.workload_dir)
    queries_path = workload_dir / "queries.json"
    if not queries_path.is_file():
        raise SystemExit(f"missing {queries_path}")
    queries = xcheck.load_json(queries_path)

    if args.dry_run:
        return run_dry_run(tier=args.tier, workload_dir=workload_dir, queries=queries)

    results_path = Path(
        args.output
        or os.environ.get(
            "OPENPUFFER_ID_OVERLAP_RESULTS",
            str(ROOT / "benchmarks" / "results" / f"id-overlap-{args.tier}.json"),
        )
    )
    if not results_path.is_absolute():
        results_path = ROOT / results_path

    if args.mock:
        fixture: Path | None = None
        if args.fixture:
            fixture = Path(args.fixture)
            if not fixture.is_absolute():
                fixture = ROOT / fixture
            if not fixture.is_file():
                raise SystemExit(f"missing mock fixture: {fixture}")
        payload = run_mock(
            tier=args.tier,
            workload_dir=workload_dir,
            queries=queries,
            fixture_path=fixture,
        )
        payload["mode"] = "mock"
    else:
        try:
            payload = run_live(
                tier=args.tier,
                workload_dir=workload_dir,
                queries=queries,
                results_path=results_path,
                timeout_sec=args.timeout_sec,
            )
        except urllib.error.URLError as exc:
            raise SystemExit(
                f"openpuffer query failed ({exc}). "
                "Set OPENPUFFER_BASE_URL or use --mock/--dry-run."
            ) from exc

    results_path.parent.mkdir(parents=True, exist_ok=True)
    with results_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")
    summary = payload.get("summary", {})
    print(
        f"wrote {results_path} mode={payload.get('mode')} "
        f"mean_overlap_at_k={summary.get('mean_overlap_at_k')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())