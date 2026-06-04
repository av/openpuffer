#!/usr/bin/env bash
# Validate committed large-dataset benchmark JSON (fixtures + *.example.json).
#
# Uses JSON Schema (python jsonschema). Ingest JSON uses structural checks aligned
# with test_ingest-timing-schema.sh (no separate schema file).
#
# Usage:
#   ./scripts/validate-benchmark-json.sh
#   ./scripts/validate-benchmark-json.sh benchmarks/report/fixtures/large-aws-l1.json
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SCHEMA_DIR="$ROOT/benchmarks/report/schema"
OP_SCHEMA="$SCHEMA_DIR/large-aws-l1.schema.json"
TPUF_SCHEMA="$SCHEMA_DIR/tpuf-l1.schema.json"

ensure_jsonschema() {
  if ! python3 -c "import jsonschema" >/dev/null 2>&1; then
    echo "validate-benchmark-json: installing jsonschema…" >&2
    python3 -m pip install --quiet --upgrade jsonschema
  fi
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
    benchmarks/results/ingest-large-l1-schema-minio.example.json \
    benchmarks/results/ingest-large-l1-schema-minio-10k.example.json; do
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

  python3 - "$OP_SCHEMA" "$TPUF_SCHEMA" "${json_paths[@]}" <<'PY'
import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator

op_schema_path = Path(sys.argv[1])
tpuf_schema_path = Path(sys.argv[2])
paths = [Path(p) for p in sys.argv[3:]]

op_validator = Draft202012Validator(json.loads(op_schema_path.read_text()))
tpuf_validator = Draft202012Validator(json.loads(tpuf_schema_path.read_text()))

INGEST_REQUIRED_TOP = (
    "benchmark",
    "environment",
    "tier",
    "workload_dir",
    "namespace",
    "num_docs",
    "dim",
    "batch_size",
    "batch_count",
    "ingest_elapsed_secs",
    "index_wait_sec",
    "ingest_total_wall_sec",
    "ingest_docs_per_sec",
    "ingest_timing",
)
INGEST_TIMING_REQUIRED = (
    "upsert_wall_sec",
    "index_wait_sec",
    "total_wall_sec",
    "batch_count",
    "batches_per_sec",
    "docs_per_sec",
    "batch_latency_ms",
    "batch_runs",
)
LAT_REQUIRED = ("p50", "p95", "min", "max")


def classify(path: Path) -> str:
    s = str(path)
    if "ingest-large" in s:
        return "ingest"
    if "tpuf" in s:
        return "tpuf"
    if "large-aws" in s:
        return "openpuffer"
    raise SystemExit(f"validate-benchmark-json: cannot classify {path}")


def require_keys(data: dict, keys: tuple, label: str, path: Path) -> None:
    missing = [k for k in keys if k not in data]
    if missing:
        raise SystemExit(
            f"validate-benchmark-json: {label} missing fields in {path}: {', '.join(missing)}"
        )


def validate_ingest(path: Path, data: dict) -> None:
    if data.get("benchmark") != "ingest_large":
        raise SystemExit(f"{path}: benchmark must be ingest_large")
    if data.get("tier") != "l1":
        raise SystemExit(f"{path}: tier must be l1")
    require_keys(data, INGEST_REQUIRED_TOP, "ingest", path)
    timing = data["ingest_timing"]
    require_keys(timing, INGEST_TIMING_REQUIRED, "ingest_timing", path)
    for key in LAT_REQUIRED:
        if key not in timing["batch_latency_ms"]:
            raise SystemExit(f"{path}: batch_latency_ms missing {key}")
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


def validate_with_schema(path: Path, data: dict, kind: str) -> None:
    if kind == "openpuffer":
        validator = op_validator
        label = "large-aws-l1"
    else:
        validator = tpuf_validator
        label = "tpuf-l1"

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


for path in paths:
    if not path.is_file():
        raise SystemExit(f"validate-benchmark-json: missing file {path}")
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"validate-benchmark-json: invalid JSON {path}: {exc}") from exc

    kind = classify(path)
    if kind == "ingest":
        validate_ingest(path, data)
    else:
        validate_with_schema(path, data, kind)
    print(f"OK {path} ({kind})")

print(f"validate-benchmark-json: {len(paths)} file(s) OK")
PY
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,10p' "$0"
  exit 0
fi

run_validation "$@"