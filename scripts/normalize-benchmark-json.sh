#!/usr/bin/env bash
# Pretty-print committed benchmark JSON for stable git diffs.
#
# Uses jq (2-space indent, ASCII escapes) for most files. Workload queries.json
# uses Python json.dumps to match generate_synthetic.py float formatting.
#
# Usage:
#   ./scripts/normalize-benchmark-json.sh              # rewrite default tracked set
#   ./scripts/normalize-benchmark-json.sh --check      # exit 1 if any file would change
#   ./scripts/normalize-benchmark-json.sh path.json    # explicit paths
#
# See benchmarks/README.md § JSON formatting & git diff
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CHECK=0
declare -a EXPLICIT=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) CHECK=1 ;;
    -h|--help)
      sed -n '2,14p' "$0"
      exit 0
      ;;
    --) shift; EXPLICIT+=("$@"); break ;;
    -*)
      echo "normalize-benchmark-json: unknown option: $1" >&2
      exit 1
      ;;
    *)
      EXPLICIT+=("$1")
      ;;
  esac
  shift
done

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "normalize-benchmark-json: missing $1" >&2
    exit 1
  }
}

need_cmd jq
need_cmd python3

# Write canonical JSON for path to stdout.
canonical_json() {
  local path="$1"
  if [[ "$(basename "$path")" == "queries.json" ]]; then
    python3 - "$path" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
sys.stdout.write(json.dumps(data, indent=2, ensure_ascii=False))
sys.stdout.write("\n")
PY
  else
    jq --indent 2 --ascii-output . "$path"
  fi
}

collect_default_paths() {
  local -a paths=()
  local f tier

  for tier in l1-100k l2-500k l3-1m; do
    for f in \
      "benchmarks/workloads/synthetic-128/${tier}/manifest.json" \
      "benchmarks/workloads/synthetic-128/${tier}/queries.json"; do
      [[ -f "$f" ]] && paths+=("$f")
    done
  done

  for f in benchmarks/report/fixtures/*.json \
    benchmarks/cross_check/fixtures/*.json \
    benchmarks/report/schema/*.json \
    benchmarks/results/*.json \
    benchmarks/results/*.example.json; do
    [[ -f "$f" ]] || continue
    case "$f" in
      benchmarks/results/OPERATOR_*|benchmarks/results/*.md) continue ;;
    esac
    paths+=("$f")
  done

  printf '%s\n' "${paths[@]}" | awk '!seen[$0]++'
}

normalize_one() {
  local path="$1"
  local canon

  if [[ ! -f "$path" ]]; then
    echo "normalize-benchmark-json: not found: $path" >&2
    return 1
  fi
  case "$path" in
    *.json) ;;
    *)
      echo "normalize-benchmark-json: skip non-JSON: $path" >&2
      return 0
      ;;
  esac

  canon="$(mktemp)"
  if ! canonical_json "$path" >"$canon"; then
    rm -f "$canon"
    echo "normalize-benchmark-json: invalid JSON: $path" >&2
    return 1
  fi

  if cmp -s "$canon" "$path"; then
    rm -f "$canon"
    return 0
  fi

  if [[ "$CHECK" -eq 1 ]]; then
    rm -f "$canon"
    echo "normalize-benchmark-json: not normalized: $path" >&2
    return 1
  fi

  cp "$canon" "$path"
  rm -f "$canon"
  echo "normalized: $path"
}

main() {
  local -a paths=()
  local p failed=0

  if [[ ${#EXPLICIT[@]} -gt 0 ]]; then
    paths=("${EXPLICIT[@]}")
  else
    mapfile -t paths < <(collect_default_paths)
  fi

  if [[ ${#paths[@]} -eq 0 ]]; then
    echo "normalize-benchmark-json: no JSON paths" >&2
    exit 1
  fi

  for p in "${paths[@]}"; do
    normalize_one "$p" || failed=1
  done

  if [[ "$failed" -ne 0 ]]; then
    if [[ "$CHECK" -eq 1 ]]; then
      echo "normalize-benchmark-json: run without --check to rewrite" >&2
    fi
    exit 1
  fi
}

main