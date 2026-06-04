#!/usr/bin/env bash
# Run shellcheck on large-dataset benchmark harness scripts (no live AWS/tpuf).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

if ! command -v shellcheck >/dev/null 2>&1; then
  fail "shellcheck not found (install shellcheck package)"
fi

shopt -s nullglob
scripts=()
for pattern in \
  scripts/*large* \
  scripts/*benchmark* \
  scripts/preflight-* \
  scripts/lib/*large* \
  scripts/lib/estimate-large-benchmark-cost.sh \
  scripts/render-report.sh \
  scripts/fill-comparison-from-report.sh \
  scripts/test_fill-comparison-from-report.sh \
  scripts/run-id-overlap* \
  scripts/run-minio-correctness-gates.sh \
  scripts/test_render-report*.sh \
  scripts/validate-benchmark-json.sh \
  scripts/check-large-aws-gates.sh \
  scripts/check-tpuf-gates.sh \
  scripts/lib/large-benchmark-aws-gates.sh \
  scripts/lib/large-benchmark-tpuf-gates.sh \
  scripts/normalize-benchmark-json.sh \
  scripts/test_normalize-benchmark-json.sh \
  scripts/test_check-large-aws-gates.sh \
  scripts/test_check-tpuf-gates.sh; do
  for f in $pattern; do
    [[ -f "$f" ]] || continue
    scripts+=("$f")
  done
done
shopt -u nullglob

if [[ "${#scripts[@]}" -eq 0 ]]; then
  fail "no benchmark scripts matched glob patterns"
fi

# De-dupe while preserving order
seen=""
unique=()
for f in "${scripts[@]}"; do
  if [[ ",${seen}," == *",${f},"* ]]; then
    continue
  fi
  seen="${seen},${f}"
  unique+=("$f")
done

echo "shellcheck benchmark scripts (${#unique[@]} files)…"
# -x: follow source= directives for lib/*.sh
shellcheck -x "${unique[@]}"

echo "shellcheck benchmark scripts OK (${#unique[@]} files)"