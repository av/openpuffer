#!/usr/bin/env bash
# Run openpuffer integration tests against real MinIO (testcontainers).
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required for MinIO testcontainers integration tests" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "error: docker daemon is not reachable" >&2
  exit 1
fi

echo "Building openpuffer with integration feature..."
cargo build --features integration

echo "Running integration tests (MinIO testcontainers)..."
cargo test -F integration

cat <<'EOF'

External S3 (optional): point at a real MinIO or AWS bucket:

  export OPENPUFFER_TEST_S3_ENDPOINT=http://127.0.0.1:9000
  export OPENPUFFER_TEST_S3_BUCKET=openpuffer-integration
  export OPENPUFFER_TEST_S3_ACCESS_KEY=minioadmin
  export OPENPUFFER_TEST_S3_SECRET_KEY=minioadmin

  cargo test -F integration --test integration_external_s3 -- --ignored

EOF