#!/usr/bin/env bash
# Run openpuffer against the docker-compose MinIO stack.
set -euo pipefail

cd "$(dirname "$0")/.."

export OPENPUFFER_S3_ENDPOINT="${OPENPUFFER_S3_ENDPOINT:-http://127.0.0.1:9000}"
export OPENPUFFER_S3_BUCKET="${OPENPUFFER_S3_BUCKET:-openpuffer-dev}"
export OPENPUFFER_S3_ACCESS_KEY="${OPENPUFFER_S3_ACCESS_KEY:-minioadmin}"
export OPENPUFFER_S3_SECRET_KEY="${OPENPUFFER_S3_SECRET_KEY:-minioadmin}"
export OPENPUFFER_S3_REGION="${OPENPUFFER_S3_REGION:-us-east-1}"

LISTEN="${OPENPUFFER_LISTEN:-0.0.0.0:8080}"

if ! curl -sf "${OPENPUFFER_S3_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
  echo "warning: MinIO does not appear reachable at ${OPENPUFFER_S3_ENDPOINT}" >&2
  echo "         Run ./scripts/dev-up.sh first" >&2
fi

echo "Serving on ${LISTEN} (bucket=${OPENPUFFER_S3_BUCKET}, endpoint=${OPENPUFFER_S3_ENDPOINT})"
exec cargo run --release -- serve \
  --listen "${LISTEN}" \
  --s3-endpoint "${OPENPUFFER_S3_ENDPOINT}" \
  --s3-bucket "${OPENPUFFER_S3_BUCKET}" \
  --s3-access-key "${OPENPUFFER_S3_ACCESS_KEY}" \
  --s3-secret-key "${OPENPUFFER_S3_SECRET_KEY}" \
  --s3-region "${OPENPUFFER_S3_REGION}"