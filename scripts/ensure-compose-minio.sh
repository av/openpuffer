#!/usr/bin/env bash
# Start docker-compose.test.yml MinIO if not already healthy (CI schema ingest / external tests).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT}/docker-compose.test.yml"
TEST_ENDPOINT="${OPENPUFFER_TEST_S3_ENDPOINT:-http://127.0.0.1:9000}"

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose -f "${COMPOSE_FILE}" "$@"
  elif command -v docker-compose >/dev/null 2>&1; then
    docker-compose -f "${COMPOSE_FILE}" "$@"
  else
    echo "error: docker compose or docker-compose not found" >&2
    exit 1
  fi
}

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "error: docker daemon required" >&2
  exit 1
fi

if curl -sf "${TEST_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
  echo "MinIO already healthy at ${TEST_ENDPOINT}"
  exit 0
fi

echo "Starting compose MinIO (${COMPOSE_FILE})..."
compose up -d
for _ in $(seq 1 60); do
  if curl -sf "${TEST_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
    compose run --rm minio-init >/dev/null
    echo "MinIO ready at ${TEST_ENDPOINT}"
    exit 0
  fi
  sleep 1
done
echo "error: MinIO did not become healthy at ${TEST_ENDPOINT}" >&2
exit 1