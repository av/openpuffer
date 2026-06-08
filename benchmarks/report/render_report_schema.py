"""Shared schema constants and validation helpers for render-report.sh.

Used by both `validate_measured_json_pair` (both sides present) and
`validate_measured_json_single` (partial report, one side only).
"""

import json
import sys
from pathlib import Path

OP_REQUIRED = (
    "benchmark", "tier", "environment", "workload_dir", "namespace",
    "namespace_docs", "dimensions", "seed", "embedding_fn",
    "p50_query_latency_ms", "p95_query_latency_ms", "recall_at_10",
    "index_cursor_eq_wal_commit_seq",
)

TPUF_REQUIRED = (
    "benchmark", "tier", "environment", "workload_dir", "namespace",
    "namespace_docs", "dimensions", "seed", "embedding_fn",
    "p50_query_latency_ms", "p95_query_latency_ms", "recall_at_10",
    "index_up_to_date",
)

MATCH_KEYS = ("tier", "namespace_docs", "dimensions", "seed", "embedding_fn")


def load_json(path: str) -> dict:
    """Load and parse a JSON file, or exit with a descriptive error."""
    p = Path(path)
    if not p.is_file():
        raise SystemExit(f"render-report: missing JSON: {path}")
    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"render-report: invalid JSON {path}: {exc}") from exc


def require_fields(data: dict, keys: tuple, label: str, path: str) -> None:
    """Exit if any required key is missing or None."""
    missing = [k for k in keys if k not in data or data[k] is None]
    if missing:
        raise SystemExit(
            f"render-report: {label} schema missing fields in {path}: "
            f"{', '.join(missing)}"
        )


def check_tier(data: dict, tier: str, label: str) -> None:
    """Exit if data['tier'] does not match the expected tier."""
    if str(data.get("tier")) != tier:
        raise SystemExit(
            f"render-report: {label} tier={data.get('tier')!r} "
            f"does not match --tier {tier}"
        )


def warn_minio(data: dict, label: str) -> None:
    """Warn on stderr if openpuffer environment contains 'minio'."""
    env = str(data.get("environment", ""))
    if "minio" in env.lower():
        print(
            f"render-report: warning {label} environment={env!r} "
            "(not aws-s3; measured COMPARISON rows expect live AWS JSON)",
            file=sys.stderr,
        )


def required_keys(side: str) -> tuple:
    """Return the required-fields tuple for 'op' or 'tpuf'."""
    return OP_REQUIRED if side == "op" else TPUF_REQUIRED


def label_for(side: str) -> str:
    """Return the human label for 'op' or 'tpuf'."""
    return "openpuffer" if side == "op" else "turbopuffer"


def validate_single(side: str, tier: str, path: str) -> dict:
    """Validate one benchmark JSON file. Returns the parsed dict."""
    lbl = label_for(side)
    data = load_json(path)
    require_fields(data, required_keys(side), lbl, path)
    check_tier(data, tier, lbl)
    if side == "op":
        warn_minio(data, lbl)
    print(
        f"render-report: schema OK tier={tier} {lbl}={path} (partial, single side)",
        file=sys.stderr,
    )
    return data


def validate_pair(tier: str, op_path: str, tpuf_path: str) -> None:
    """Validate both benchmark JSON files and cross-check workload keys."""
    op = load_json(op_path)
    tpuf = load_json(tpuf_path)
    require_fields(op, OP_REQUIRED, "openpuffer", op_path)
    require_fields(tpuf, TPUF_REQUIRED, "turbopuffer", tpuf_path)
    check_tier(op, tier, "openpuffer")
    check_tier(tpuf, tier, "turbopuffer")

    for key in MATCH_KEYS:
        if op.get(key) != tpuf.get(key):
            raise SystemExit(
                f"render-report: workload mismatch on {key}: "
                f"openpuffer={op.get(key)!r} turbopuffer={tpuf.get(key)!r}"
            )

    warn_minio(op, "openpuffer")
    print(
        f"render-report: schema OK tier={tier} op={op_path} tpuf={tpuf_path}",
        file=sys.stderr,
    )
