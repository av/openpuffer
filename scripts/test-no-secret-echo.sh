#!/usr/bin/env bash
# Grep-based gate: benchmark harness must not echo API keys / S3 secrets to stdout/stderr.
# Catches accidental "echo $KEY", credential export lines, key-prefix leaks, and curl -v/-u.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

if ! command -v rg >/dev/null 2>&1; then
  fail "ripgrep (rg) not found"
fi

shopt -s nullglob
files=()
for pattern in \
  scripts/*large* \
  scripts/*benchmark* \
  scripts/preflight-* \
  scripts/lib/*large* \
  scripts/lib/estimate-large-benchmark-cost.sh \
  scripts/render-report.sh \
  scripts/run-id-overlap* \
  scripts/run-minio-correctness-gates.sh \
  scripts/test_render-report*.sh \
  scripts/validate-benchmark-json.sh \
  benchmarks/tpuf_driver/*.py \
  benchmarks/cross_check/*.py \
  .github/workflows/benchmark-large*.yml; do
  for f in $pattern; do
    [[ -f "$f" ]] || continue
    files+=("$f")
  done
done
shopt -u nullglob

if [[ "${#files[@]}" -eq 0 ]]; then
  fail "no benchmark files matched scan globs"
fi

# De-dupe while preserving order
seen=""
unique=()
for f in "${files[@]}"; do
  if [[ ",${seen}," == *",${f},"* ]]; then
    continue
  fi
  seen="${seen},${f}"
  unique+=("$f")
done

# Patterns that must not appear in harness output paths (see scripts/preflight-tpuf.sh, preflight-aws-ec2.sh).
# Allowed elsewhere: =set / =unset, :? errors, tpuf_... placeholders, ::add-mask:: in CI.
PATTERNS=(
  'echo[^#\n]*\$\{?TURBOPUFFER_API_KEY[^:?=]'
  'echo[^#\n]*\$\{?OPENPUFFER_S3_SECRET_KEY'
  'echo[^#\n]*\$\{?OPENPUFFER_S3_ACCESS_KEY'
  'echo[^#\n]*\$\{?AWS_SESSION_TOKEN'
  'echo[^#\n]*export[[:space:]]+OPENPUFFER_S3_(ACCESS|SECRET)_KEY=.*\$\{'
  'key:0:'
  'curl[[:space:]].*(-v|--verbose)'
  'curl[[:space:]].*(-u|--user)[[:space:]]*\$'
  'print\([^)]*api_key'
  'print\([^)]*\{api_key\}'
  'print\([^)]*\{key\}'
)

violations=0
for pat in "${PATTERNS[@]}"; do
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    # Skip safe status lines and operator placeholders
    if [[ "$line" == *'=set'* || "$line" == *'=unset'* || "$line" == *'tpuf_...'* ]]; then
      continue
    fi
    if [[ "$line" == *'::add-mask::'* ]]; then
      continue
    fi
    echo "$line" >&2
    violations=$((violations + 1))
  done < <(rg -n --no-heading -e "$pat" "${unique[@]}" 2>/dev/null || true)
done

if [[ "$violations" -gt 0 ]]; then
  fail "${violations} possible secret-echo match(es) in benchmark harness (see above)"
fi

echo "test-no-secret-echo: OK (${#unique[@]} files, ${#PATTERNS[@]} patterns)"