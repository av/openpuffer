#!/usr/bin/env bash
# Offline tests for scripts/check-benchmark-artifacts.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CHECK="$ROOT/scripts/check-benchmark-artifacts.sh"
chmod +x "$CHECK"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

echo "==> tracked artifacts (repo policy)"
"$CHECK"

echo "==> reject minio env on live-shaped basename"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
cat >"$tmpdir/large-aws-l1.json" <<'EOF'
{"benchmark":"cold_large_l1","environment":"minio","tier":"l1"}
EOF
if "$CHECK" "$tmpdir/large-aws-l1.json" >/dev/null 2>&1; then
  fail "expected failure for minio large-aws-l1.json"
fi

echo "==> accept aws-s3 on live-shaped basename"
cat >"$tmpdir/large-aws-l1.json" <<'EOF'
{"benchmark":"cold_large_l1","environment":"aws-s3","tier":"l1"}
EOF
"$CHECK" "$tmpdir/large-aws-l1.json" >/dev/null

echo "==> reject aws-s3 on schema-minio-shaped basename"
cat >"$tmpdir/large-aws-l1-schema-minio.example.json" <<'EOF'
{"benchmark":"cold_large_l1","environment":"aws-s3","tier":"l1"}
EOF
if "$CHECK" "$tmpdir/large-aws-l1-schema-minio.example.json" >/dev/null 2>&1; then
  fail "expected failure for aws-s3 on schema-minio example"
fi

echo "==> accept minio on schema-minio-shaped basename"
cat >"$tmpdir/large-aws-l1-schema-minio.example.json" <<'EOF'
{"benchmark":"cold_large_l1","environment":"minio","tier":"l1"}
EOF
"$CHECK" "$tmpdir/large-aws-l1-schema-minio.example.json" >/dev/null

echo "==> reject minio env on live tpuf basename"
cat >"$tmpdir/tpuf-l1.json" <<'EOF'
{"benchmark":"cold_tpuf_l1","environment":"minio","tier":"l1"}
EOF
if "$CHECK" "$tmpdir/tpuf-l1.json" >/dev/null 2>&1; then
  fail "expected failure for minio tpuf-l1.json"
fi

echo "==> accept turbopuffer env on live tpuf basename"
cat >"$tmpdir/tpuf-l1.json" <<'EOF'
{"benchmark":"cold_tpuf_l1","environment":"turbopuffer:aws-us-east-1","tier":"l1"}
EOF
"$CHECK" "$tmpdir/tpuf-l1.json" >/dev/null

echo "test_check-benchmark-artifacts: OK"