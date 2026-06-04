#!/usr/bin/env bash
# Git policy gate for benchmarks/results/ (and report/fixtures when tracked).
#
# Fails if comparison-grade JSON is tracked (or passed explicitly) with wrong
# environment for its artifact class:
#   - live large-aws-* / ingest-large-* (non-example, non-schema-minio) → aws-s3
#   - live tpuf-* → environment must start with turbopuffer:
#   - *schema-minio* / *.example.json with openpuffer ingest/bench → minio only
#
# Live measured paths are listed in .gitignore; operators commit after EC2 runs:
#   git add -f benchmarks/results/large-aws-l1.json
#
# Usage:
#   ./scripts/check-benchmark-artifacts.sh
#   ./scripts/check-benchmark-artifacts.sh --staged
#   ./scripts/check-benchmark-artifacts.sh benchmarks/results/large-aws-l1.json
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

STAGED=0
declare -a EXPLICIT=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --staged) STAGED=1 ;;
    -h|--help)
      sed -n '2,18p' "$0"
      exit 0
      ;;
    -*)
      echo "check-benchmark-artifacts: unknown option: $1" >&2
      exit 1
      ;;
    *)
      EXPLICIT+=("$1")
      ;;
  esac
  shift
done

# Legacy G2 snapshots (MinIO/testcontainers); committed intentionally.
LEGACY_MINIO_OK=(
  benchmarks/results/baseline-10k.json
  benchmarks/results/cold-50k-v3.json
  benchmarks/results/nightly-100k.json
)

is_op_scaling_snapshot() {
  local base="$1"
  [[ "$base" =~ ^op-scaling-.*\.json$ ]]
}

is_legacy_minio_ok() {
  local rel="$1"
  local p
  for p in "${LEGACY_MINIO_OK[@]}"; do
    [[ "$rel" == "$p" ]] && return 0
  done
  return 1
}

is_exempt_live_gate() {
  local base="$1"
  [[ "$base" == *.example.json ]] && return 0
  [[ "$base" == *schema-minio* ]] && return 0
  return 1
}

is_minio_shape_path() {
  local base="$1"
  [[ "$base" == *schema-minio* ]] || [[ "$base" == *.example.json && "$base" == *minio* ]]
}

is_live_openpuffer_bench() {
  local base="$1"
  [[ "$base" =~ ^large-aws-l[123]\.json$ ]]
}

is_live_openpuffer_ingest() {
  local base="$1"
  [[ "$base" =~ ^ingest-large-l[123]\.json$ ]]
}

is_live_tpuf() {
  local base="$1"
  [[ "$base" =~ ^tpuf-l[123]\.json$ ]]
}

is_live_id_overlap() {
  local base="$1"
  [[ "$base" =~ ^id-overlap-l[123]\.json$ ]]
}

json_environment() {
  local path="$1"
  if ! command -v jq >/dev/null 2>&1; then
    echo "check-benchmark-artifacts: jq required" >&2
    exit 1
  fi
  jq -r '.environment // empty' "$path" 2>/dev/null || echo ""
}

collect_tracked_paths() {
  local -a paths=()
  local rel
  while IFS= read -r rel; do
    [[ -n "$rel" ]] || continue
    paths+=("$rel")
  done < <(
    {
      git ls-files 'benchmarks/results/*.json' 2>/dev/null || true
      git ls-files 'benchmarks/report/fixtures/*.json' 2>/dev/null || true
      if [[ "$STAGED" == "1" ]]; then
        git diff --cached --name-only --diff-filter=ACMR 2>/dev/null | grep -E '^benchmarks/(results|report/fixtures)/.*\.json$' || true
      fi
    } | sort -u
  )
  if [[ "${#paths[@]}" -eq 0 && "${#EXPLICIT[@]}" -eq 0 ]]; then
    echo "check-benchmark-artifacts: no JSON under benchmarks/results or report/fixtures" >&2
    exit 1
  fi
  printf '%s\0' "${paths[@]}"
}

fail() {
  echo "check-benchmark-artifacts: $*" >&2
  exit 1
}

check_file() {
  local rel="$1"
  [[ -f "$rel" ]] || fail "missing file: $rel"
  local base
  base="$(basename "$rel")"

  if [[ "$base" != *.json ]]; then
    return 0
  fi

  local env
  env="$(json_environment "$rel")"

  if is_legacy_minio_ok "$rel"; then
    echo "  OK (legacy G2) $rel environment=${env:-<unset>}"
    return 0
  fi

  if is_op_scaling_snapshot "$base"; then
    [[ "$env" == "minio-testcontainers" || "$env" == minio-* ]] \
      || fail "$rel: op-scaling snapshots must use environment=minio-testcontainers (got ${env:-<unset>})"
    echo "  OK (op-scaling) $rel environment=${env}"
    return 0
  fi

  if [[ "$rel" == benchmarks/report/fixtures/* ]]; then
    if [[ "$base" == large-aws-* ]] || [[ "$base" == ingest-large-* ]]; then
      [[ "$env" == "aws-s3" ]] || fail "$rel: report fixture openpuffer JSON must have environment=aws-s3 (got ${env:-<unset>})"
    elif [[ "$base" == tpuf-* ]]; then
      [[ "$env" == turbopuffer:* ]] || fail "$rel: report fixture tpuf JSON must have environment=turbopuffer:<region> (got ${env:-<unset>})"
    fi
    echo "  OK (fixture) $rel"
    return 0
  fi

  if [[ "$rel" != benchmarks/results/* ]]; then
    # Paths outside results/ (explicit test inputs): enforce basename policy only.
    if ! is_live_openpuffer_bench "$base" && ! is_live_openpuffer_ingest "$base" \
      && ! is_live_tpuf "$base" && ! is_live_id_overlap "$base" \
      && ! is_minio_shape_path "$base" && [[ "$base" != *.example.json ]]; then
      return 0
    fi
  fi

  if is_minio_shape_path "$base"; then
    [[ "$env" == "minio" ]] || fail "$rel: MinIO schema/example JSON must have environment=minio (got ${env:-<unset>})"
    echo "  OK (minio shape) $rel"
    return 0
  fi

  if is_exempt_live_gate "$base"; then
    if [[ "$base" == large-aws-* ]] || [[ "$base" == ingest-large-* ]]; then
      [[ "$env" == "aws-s3" ]] || fail "$rel: *.example.json openpuffer placeholders use environment=aws-s3 (got ${env:-<unset>})"
    elif [[ "$base" == tpuf-* ]]; then
      [[ "$env" == turbopuffer:* ]] || fail "$rel: *.example.json tpuf placeholders use environment=turbopuffer:<region> (got ${env:-<unset>})"
    fi
    echo "  OK (example) $rel"
    return 0
  fi

  if is_live_openpuffer_bench "$base" || is_live_openpuffer_ingest "$base"; then
    [[ "$env" == "aws-s3" ]] || fail "$rel: live openpuffer comparison JSON must have environment=aws-s3 (got ${env:-<unset>}); MinIO runs belong in *-schema-minio*.example.json"
    echo "  OK (live openpuffer) $rel environment=aws-s3"
    return 0
  fi

  if is_live_tpuf "$base"; then
    [[ "$env" == turbopuffer:* ]] || fail "$rel: live tpuf JSON must have environment=turbopuffer:<region> (got ${env:-<unset>})"
    echo "  OK (live tpuf) $rel"
    return 0
  fi

  if is_live_id_overlap "$base"; then
    echo "  OK (live id-overlap) $rel"
    return 0
  fi

  # Other results/*.json (e.g. 1m-aws.json): forbid minio-class env in comparison filenames.
  if [[ -n "$env" && ( "$env" == "minio" || "$env" == minio-* ) ]]; then
    fail "$rel: environment=${env} is not committable under a non-exempt results name; use *-schema-minio*.example.json or legacy snapshots only"
  fi
  echo "  OK $rel"
}

main() {
  local -a to_check=()
  if [[ "${#EXPLICIT[@]}" -gt 0 ]]; then
    to_check=("${EXPLICIT[@]}")
  else
    while IFS= read -r -d '' rel; do
      to_check+=("$rel")
    done < <(collect_tracked_paths)
  fi

  echo "check-benchmark-artifacts: scanning ${#to_check[@]} file(s)…"
  local rel
  for rel in "${to_check[@]}"; do
    check_file "$rel"
  done
  echo "check-benchmark-artifacts: OK"
}

main