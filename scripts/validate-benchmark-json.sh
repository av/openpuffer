#!/usr/bin/env bash
# Validate committed large-dataset benchmark JSON (fixtures + *.example.json).
#
# Uses JSON Schema (python jsonschema) for large-aws, tpuf, ingest-large, and id-overlap
# artifacts across tiers L1–L3. Ingest and overlap JSON also get cross-field checks after schema pass.
#
# Usage:
#   ./scripts/validate-benchmark-json.sh
#   ./scripts/validate-benchmark-json.sh benchmarks/results/large-aws-l2.example.json
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# shellcheck source=scripts/lib/benchmark-python-deps.sh
source "$ROOT/scripts/lib/benchmark-python-deps.sh"

SCHEMA_DIR="$ROOT/benchmarks/report/schema"
OP_SCHEMA="$SCHEMA_DIR/large-aws-l1.schema.json"
TPUF_SCHEMA="$SCHEMA_DIR/tpuf-l1.schema.json"
INGEST_SCHEMA="$SCHEMA_DIR/ingest-large-l1.schema.json"
OVERLAP_SCHEMA="$SCHEMA_DIR/id-overlap-l1.schema.json"

ensure_jsonschema() {
  ensure_benchmark_python_deps "$ROOT"
}

collect_default_paths() {
  local -a paths=()
  local f
  for f in \
    benchmarks/report/fixtures/large-aws-l1.json \
    benchmarks/report/fixtures/tpuf-l1.json \
    benchmarks/report/fixtures/ingest-large-l1.json \
    benchmarks/results/large-aws-l1-schema-minio.example.json \
    benchmarks/results/large-aws-l1-schema-minio-10k.example.json \
    benchmarks/results/large-aws-l2.example.json \
    benchmarks/results/large-aws-l3.example.json \
    benchmarks/results/ingest-large-l1-schema-minio.example.json \
    benchmarks/results/ingest-large-l1-schema-minio-10k.example.json \
    benchmarks/results/ingest-large-l2.example.json \
    benchmarks/results/ingest-large-l3.example.json \
    benchmarks/results/tpuf-l2.example.json \
    benchmarks/results/tpuf-l3.example.json \
    benchmarks/results/id-overlap-l1.example.json; do
    if [[ -f "$f" ]]; then
      paths+=("$f")
    fi
  done
  if [[ "${#paths[@]}" -eq 0 ]]; then
    echo "validate-benchmark-json: no default JSON files found" >&2
    exit 1
  fi
  printf '%s\0' "${paths[@]}"
}

run_validation() {
  ensure_jsonschema

  local -a json_paths=()
  if [[ "$#" -gt 0 ]]; then
    json_paths=("$@")
  else
    while IFS= read -r -d '' p; do
      json_paths+=("$p")
    done < <(collect_default_paths)
  fi

  python3 - "$OP_SCHEMA" "$TPUF_SCHEMA" "$INGEST_SCHEMA" "$OVERLAP_SCHEMA" "${json_paths[@]}" <<'PY'
import json
import re
import sys
from pathlib import Path

from jsonschema import Draft202012Validator

op_schema_path = Path(sys.argv[1])
tpuf_schema_path = Path(sys.argv[2])
ingest_schema_path = Path(sys.argv[3])
overlap_schema_path = Path(sys.argv[4])
paths = [Path(p) for p in sys.argv[5:]]

sys.path.insert(0, str(op_schema_path.parent.parent))
from utc_timestamps import validate_benchmark_timestamps

op_validator = Draft202012Validator(json.loads(op_schema_path.read_text()))
tpuf_validator = Draft202012Validator(json.loads(tpuf_schema_path.read_text()))
ingest_validator = Draft202012Validator(json.loads(ingest_schema_path.read_text()))
overlap_validator = Draft202012Validator(json.loads(overlap_schema_path.read_text()))

SCHEMA_VERSION_PATH = Path(op_schema_path).parent / ".." / "LARGE_BENCHMARK_JSON_SCHEMA_VERSION"
EXPECTED_SCHEMA_VERSION = SCHEMA_VERSION_PATH.resolve().read_text(encoding="utf-8").strip()

TIER_META = {
    "l1": {
        "docs": 100_000,
        "workload_dir": "benchmarks/workloads/synthetic-128/l1-100k",
        "cold_large": "cold_large_l1",
        "cold_tpuf": "cold_tpuf_l1",
    },
    "l2": {
        "docs": 500_000,
        "workload_dir": "benchmarks/workloads/synthetic-128/l2-500k",
        "cold_large": "cold_large_l2",
        "cold_tpuf": "cold_tpuf_l2",
    },
    "l3": {
        "docs": 1_000_000,
        "workload_dir": "benchmarks/workloads/synthetic-128/l3-1m",
        "cold_large": "cold_large_l3",
        "cold_tpuf": "cold_tpuf_l3",
    },
}


def classify(path: Path) -> str:
    s = str(path)
    if "id-overlap" in s:
        return "id-overlap"
    if "ingest-large" in s:
        return "ingest"
    if "tpuf" in s:
        return "tpuf"
    if "large-aws" in s:
        return "openpuffer"
    raise SystemExit(f"validate-benchmark-json: cannot classify {path}")


def infer_tier_from_path(path: Path) -> str | None:
    m = re.search(r"-l([123])(?:\.json|[-.])", path.name)
    if m:
        return f"l{m.group(1)}"
    return None


def skip_doc_tier_check(path: Path) -> bool:
    """MinIO CI fast-path examples use tier=l1 workload but fewer docs (e.g. 10k)."""
    name = path.name
    return "10k" in name or "schema-minio-10k" in name


def validate_schema_version(path: Path, data: dict) -> None:
    sv = data.get("schema_version")
    if sv != EXPECTED_SCHEMA_VERSION:
        raise SystemExit(
            f"{path}: schema_version {sv!r} != {EXPECTED_SCHEMA_VERSION!r} "
            "(regenerate with ingest-large / bench-large / tpuf driver / id-overlap)"
        )


def validate_utc_timestamps(path: Path, data: dict) -> None:
    try:
        validate_benchmark_timestamps(data)
    except ValueError as exc:
        raise SystemExit(f"{path}: {exc}") from exc


def validate_tier_alignment(path: Path, kind: str, data: dict) -> None:
    tier = data.get("tier")
    if tier not in TIER_META:
        raise SystemExit(f"{path}: unknown tier {tier!r} (expected l1, l2, or l3)")
    meta = TIER_META[tier]

    path_tier = infer_tier_from_path(path)
    if path_tier and path_tier != tier:
        raise SystemExit(f"{path}: filename implies tier {path_tier} but JSON has tier={tier}")

    workload = data.get("workload_dir")
    if workload and workload != meta["workload_dir"]:
        raise SystemExit(
            f"{path}: workload_dir {workload!r} != expected {meta['workload_dir']!r} for tier {tier}"
        )

    if kind == "ingest":
        expected_benchmark = "ingest_large"
        if data.get("benchmark") != expected_benchmark:
            raise SystemExit(
                f"{path}: benchmark {data.get('benchmark')!r} != {expected_benchmark!r}"
            )
        num_docs = data.get("num_docs")
        if (
            num_docs is not None
            and num_docs != meta["docs"]
            and not skip_doc_tier_check(path)
        ):
            raise SystemExit(
                f"{path}: num_docs {num_docs} != {meta['docs']} for tier {tier}"
            )
        batch_count = data.get("batch_count")
        if batch_count is not None and num_docs is not None and not skip_doc_tier_check(path):
            batch_size = data.get("batch_size") or 10_000
            expected_batches = (num_docs + batch_size - 1) // batch_size
            if batch_count != expected_batches:
                raise SystemExit(
                    f"{path}: batch_count {batch_count} != ceil({num_docs}/{batch_size})={expected_batches}"
                )
        return

    if kind == "id-overlap":
        if data.get("benchmark") != "id_overlap_spotcheck":
            raise SystemExit(f"{path}: benchmark must be id_overlap_spotcheck")
        return

    if kind == "openpuffer":
        expected_benchmark = meta["cold_large"]
        if data.get("benchmark") != expected_benchmark:
            raise SystemExit(
                f"{path}: benchmark {data.get('benchmark')!r} != {expected_benchmark!r}"
            )
        docs = data.get("namespace_docs")
        if (
            docs is not None
            and docs != meta["docs"]
            and not skip_doc_tier_check(path)
        ):
            raise SystemExit(
                f"{path}: namespace_docs {docs} != {meta['docs']} for tier {tier}"
            )
        return

    # tpuf
    expected_benchmark = meta["cold_tpuf"]
    if data.get("benchmark") != expected_benchmark:
        raise SystemExit(
            f"{path}: benchmark {data.get('benchmark')!r} != {expected_benchmark!r}"
        )
    docs = data.get("namespace_docs")
    if (
        docs is not None
        and docs != meta["docs"]
        and not skip_doc_tier_check(path)
    ):
        raise SystemExit(
            f"{path}: namespace_docs {docs} != {meta['docs']} for tier {tier}"
        )


def schema_errors(validator, data, label: str, path: Path) -> None:
    errors = sorted(validator.iter_errors(data), key=lambda e: list(e.path))
    if errors:
        lines = [f"  - {e.message} (at {list(e.path)})" for e in errors[:12]]
        extra = len(errors) - 12
        suffix = f"\n  … and {extra} more" if extra > 0 else ""
        raise SystemExit(
            f"validate-benchmark-json: {label} schema failed for {path}:\n"
            + "\n".join(lines)
            + suffix
        )


def validate_overlap_cross_fields(path: Path, data: dict) -> None:
    validate_tier_alignment(path, "id-overlap", data)
    queries = data.get("queries") or []
    summary = data.get("summary") or {}
    if summary.get("query_count") != len(queries):
        raise SystemExit(
            f"{path}: summary.query_count != len(queries) "
            f"({summary.get('query_count')} vs {len(queries)})"
        )
    spot = data.get("spot_check") or {}
    top_k = int(spot.get("top_k", 10))
    for row in queries:
        if row.get("top_k") != top_k:
            raise SystemExit(f"{path}: query {row.get('name')} top_k != spot_check.top_k")
        inter_n = int(row.get("intersection_count", 0))
        expected = round(inter_n / max(top_k, 1), 4)
        actual = row.get("overlap_at_k")
        if actual is not None and abs(float(actual) - expected) > 0.0001:
            raise SystemExit(
                f"{path}: query {row.get('name')} overlap_at_k {actual} != {expected}"
            )


def validate_openpuffer_ann_gate(path: Path, data: dict) -> None:
    """Large-dataset program requires v3 index (OPENPUFFER_ANN_VERSION=3)."""
    pref = data.get("preferred_ann_version")
    if pref != 3:
        raise SystemExit(
            f"{path}: preferred_ann_version must be 3 (got {pref!r}); "
            "re-ingest with OPENPUFFER_ANN_VERSION=3 — see docs/BENCHMARKS.md#ann-index-v3-gate-openpuffer_ann_version3"
        )
    if data.get("index_cursor_eq_wal_commit_seq") is not True:
        raise SystemExit(
            f"{path}: index_cursor_eq_wal_commit_seq must be true (got {data.get('index_cursor_eq_wal_commit_seq')!r})"
        )


def validate_ingest_ann_gate(path: Path, data: dict) -> None:
    pref = data.get("preferred_ann_version")
    if pref != 3:
        raise SystemExit(
            f"{path}: preferred_ann_version must be 3 (got {pref!r}); "
            "serve must use OPENPUFFER_ANN_VERSION=3 before ingest"
        )
    if data.get("index_cursor_eq_wal_commit_seq") is not True:
        raise SystemExit(
            f"{path}: index_cursor_eq_wal_commit_seq must be true (got {data.get('index_cursor_eq_wal_commit_seq')!r})"
        )


def validate_ingest_cross_fields(path: Path, data: dict) -> None:
    validate_tier_alignment(path, "ingest", data)
    validate_ingest_ann_gate(path, data)
    timing = data["ingest_timing"]
    if data["ingest_elapsed_secs"] != timing["upsert_wall_sec"]:
        raise SystemExit(f"{path}: ingest_elapsed_secs != ingest_timing.upsert_wall_sec")
    if data["index_wait_sec"] != timing["index_wait_sec"]:
        raise SystemExit(f"{path}: index_wait_sec mismatch")
    if data["ingest_total_wall_sec"] != timing["total_wall_sec"]:
        raise SystemExit(f"{path}: ingest_total_wall_sec mismatch")
    if not isinstance(timing["batch_runs"], list):
        raise SystemExit(f"{path}: ingest_timing.batch_runs must be a list")
    if timing["batch_runs"]:
        run0 = timing["batch_runs"][0]
        if not {"batch", "file", "latency_ms"} <= set(run0.keys()):
            raise SystemExit(f"{path}: batch_runs[0] missing batch/file/latency_ms")


for path in paths:
    if not path.is_file():
        raise SystemExit(f"validate-benchmark-json: missing file {path}")
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"validate-benchmark-json: invalid JSON {path}: {exc}") from exc

    kind = classify(path)
    validate_schema_version(path, data)
    validate_utc_timestamps(path, data)
    if kind == "ingest":
        schema_errors(ingest_validator, data, "ingest-large", path)
        validate_ingest_cross_fields(path, data)
    elif kind == "id-overlap":
        schema_errors(overlap_validator, data, "id-overlap", path)
        validate_overlap_cross_fields(path, data)
    elif kind == "openpuffer":
        schema_errors(op_validator, data, "large-aws", path)
        validate_tier_alignment(path, kind, data)
        validate_openpuffer_ann_gate(path, data)
    else:
        schema_errors(tpuf_validator, data, "tpuf", path)
        validate_tier_alignment(path, kind, data)
    print(f"OK {path} ({kind}, tier={data.get('tier', '?')})")

print(f"validate-benchmark-json: {len(paths)} file(s) OK")
PY
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,10p' "$0"
  exit 0
fi

run_validation "$@"