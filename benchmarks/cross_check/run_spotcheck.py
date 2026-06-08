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

def _rel_workload(workload_dir: Path) -> str:
    """Relative path string if under ROOT, else absolute."""
    if workload_dir.is_relative_to(ROOT):
        return str(workload_dir.relative_to(ROOT))
    return str(workload_dir)


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


def fetch_openpuffer_meta(
    base_url: str,
    namespace: str,
    *,
    timeout_sec: int = 30,
) -> tuple[dict[str, Any] | None, str | None]:
    """GET /v1/namespaces/{name}. Returns (meta, error_hint)."""
    url = f"{base_url.rstrip('/')}/v1/namespaces/{namespace}"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            return json.loads(resp.read().decode("utf-8")), None
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None, f"HTTP 404 — namespace {namespace!r} does not exist at {url}"
        body = exc.read().decode("utf-8", errors="replace")[:200]
        return None, f"HTTP {exc.code} from {url}: {body}"
    except urllib.error.URLError as exc:
        return None, (
            f"cannot reach openpuffer at {base_url!r} ({exc}). "
            "Is serve running? Set OPENPUFFER_BASE_URL or use --mock/--dry-run."
        )


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


def live_namespace_env(*, tier: str) -> dict[str, str]:
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
    return {
        "openpuffer_base_url": op_base,
        "openpuffer_namespace": op_ns,
        "turbopuffer_region": tpuf_region,
        "turbopuffer_namespace": tpuf_ns,
    }


def preflight_live_namespaces(
    *,
    tier: str,
    workload_dir: Path,
    timeout_sec: int,
) -> None:
    """Exit with actionable errors when either side has no indexed docs."""
    manifest_path = workload_dir / "manifest.json"
    manifest: dict[str, Any] | None = None
    if manifest_path.is_file():
        manifest = xcheck.load_json(manifest_path)
    expected_docs = xcheck.expected_docs_for_tier(tier, manifest)

    env = live_namespace_env(tier=tier)
    op_base = env["openpuffer_base_url"]
    op_ns = env["openpuffer_namespace"]
    tpuf_ns = env["turbopuffer_namespace"]
    tpuf_region = env["turbopuffer_region"]

    api_key = os.environ.get("TURBOPUFFER_API_KEY")
    if not api_key:
        raise SystemExit(
            "id-overlap live preflight: TURBOPUFFER_API_KEY unset "
            "(or use --mock / --dry-run)"
        )

    op_meta, op_fetch_err = fetch_openpuffer_meta(
        op_base, op_ns, timeout_sec=timeout_sec
    )
    op_issues = xcheck.openpuffer_namespace_issues(
        op_meta, expected_docs=expected_docs, namespace=op_ns
    )
    if op_fetch_err:
        op_issues = [op_fetch_err, *op_issues]
    if op_issues:
        raise SystemExit(
            xcheck.format_namespace_preflight_error(
                engine="openpuffer",
                namespace=op_ns,
                issues=op_issues,
                tier=tier,
            )
        )

    try:
        from turbopuffer import Turbopuffer
    except ImportError as exc:
        raise SystemExit(
            "pip install -r benchmarks/requirements.txt"
        ) from exc

    client = Turbopuffer(api_key=api_key, region=tpuf_region)
    ns = client.namespace(tpuf_ns)
    try:
        tpuf_meta = ns.metadata()
    except Exception as exc:  # noqa: BLE001 — SDK errors vary by version
        raise SystemExit(
            xcheck.format_namespace_preflight_error(
                engine="turbopuffer",
                namespace=tpuf_ns,
                issues=[
                    f"metadata() failed: {exc}",
                    "namespace may not exist — confirm TURBOPUFFER_BENCH_NAMESPACE "
                    "matches the namespace used by run-tpuf-large-benchmark.sh",
                ],
                tier=tier,
            )
        ) from exc

    tpuf_issues = xcheck.turbopuffer_namespace_issues(
        tpuf_meta, expected_docs=expected_docs, namespace=tpuf_ns
    )
    if tpuf_issues:
        raise SystemExit(
            xcheck.format_namespace_preflight_error(
                engine="turbopuffer",
                namespace=tpuf_ns,
                issues=tpuf_issues,
                tier=tier,
            )
        )

    op_rows = int((op_meta or {}).get("approx_row_count") or 0)
    tpuf_rows = getattr(tpuf_meta, "approx_row_count", None)
    print(
        f"id-overlap preflight OK tier={tier} "
        f"openpuffer={op_ns} rows≈{op_rows} "
        f"turbopuffer={tpuf_ns} rows≈{tpuf_rows}"
    )


def run_mock(
    *,
    tier: str,
    workload_dir: Path,
    queries: dict[str, Any],
    fixture_path: Path | None,
) -> dict[str, Any]:
    rel_workload = _rel_workload(workload_dir)
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

    env = live_namespace_env(tier=tier)
    op_base = env["openpuffer_base_url"]
    op_ns = env["openpuffer_namespace"]
    tpuf_region = env["turbopuffer_region"]
    tpuf_ns = env["turbopuffer_namespace"]

    started_at = utc_now_iso()

    preflight_live_namespaces(
        tier=tier, workload_dir=workload_dir, timeout_sec=min(timeout_sec, 60)
    )

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
        try:
            op_resp = openpuffer_query(
                op_base, op_ns, op_body, timeout_sec=timeout_sec
            )
        except urllib.error.HTTPError as exc:
            raise SystemExit(
                f"openpuffer query failed for {name!r} on namespace {op_ns!r}: "
                f"HTTP {exc.code}. Re-run ./scripts/preflight-id-overlap.sh --tier {tier}"
            ) from exc
        except urllib.error.URLError as exc:
            raise SystemExit(
                f"openpuffer query failed for {name!r} ({exc}). "
                f"Check OPENPUFFER_BASE_URL={op_base!r} and namespace {op_ns!r}."
            ) from exc
        op_ids = xcheck.extract_ids_openpuffer_response(op_resp)
        if not op_ids:
            raise SystemExit(
                xcheck.format_namespace_preflight_error(
                    engine="openpuffer",
                    namespace=op_ns,
                    issues=[
                        f"query {name!r} returned 0 ids (top_k={top_k}) — "
                        "namespace likely empty or index not serving ANN results",
                    ],
                    tier=tier,
                )
            )

        try:
            tpuf_resp = ns.query(
                **xcheck.turbopuffer_query_kwargs(
                    vector,
                    top_k=top_k,
                    consistency=consistency,
                    include_attributes=include_attrs,
                )
            )
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(
                xcheck.format_namespace_preflight_error(
                    engine="turbopuffer",
                    namespace=tpuf_ns,
                    issues=[f"query {name!r} failed: {exc}"],
                    tier=tier,
                )
            ) from exc
        tpuf_ids = xcheck.extract_ids_tpuf_response(tpuf_resp)
        if not tpuf_ids:
            raise SystemExit(
                xcheck.format_namespace_preflight_error(
                    engine="turbopuffer",
                    namespace=tpuf_ns,
                    issues=[
                        f"query {name!r} returned 0 ids (top_k={top_k}) — "
                        "namespace likely empty or not indexed",
                    ],
                    tier=tier,
                )
            )
        metrics = xcheck.overlap_metrics(op_ids, tpuf_ids, top_k=top_k)
        per_query.append(
            {
                "name": name,
                "doc_index": spec.get("doc_index"),
                **metrics,
            }
        )

    rel_workload = _rel_workload(workload_dir)
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
    rel = _rel_workload(workload_dir)
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
        "--preflight-only",
        action="store_true",
        help="Check both namespaces are indexed (no spot-check queries)",
    )
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

    if args.preflight_only:
        preflight_live_namespaces(
            tier=args.tier,
            workload_dir=workload_dir,
            timeout_sec=args.timeout_sec,
        )
        return 0

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
        except (urllib.error.URLError, urllib.error.HTTPError) as exc:
            hint = (
                f"openpuffer spot-check failed ({exc}). "
                f"Run: ./scripts/preflight-id-overlap.sh --tier {args.tier} "
                "or use --mock/--dry-run."
            )
            raise SystemExit(hint) from exc

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