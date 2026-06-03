#!/usr/bin/env bash
# Start MinIO (and create openpuffer-dev bucket) for local development.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required (install Docker or Podman with compose)" >&2
  exit 1
fi

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  elif command -v docker-compose >/dev/null 2>&1; then
    docker-compose "$@"
  else
    echo "error: docker compose or docker-compose not found" >&2
    exit 1
  fi
}

if ! docker info >/dev/null 2>&1; then
  echo "error: docker daemon is not reachable" >&2
  exit 1
fi

echo "Starting MinIO (S3 API :9000, console :9001)..."
compose up -d

echo "Waiting for MinIO..."
for _ in $(seq 1 60); do
  if curl -sf http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -sf http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1; then
  echo "error: MinIO did not become healthy on :9000" >&2
  exit 1
fi

# Idempotent bucket create (no-op if minio-init already ran).
compose run --rm minio-init >/dev/null

cat <<'EOF'

MinIO is up:
  S3 API:    http://127.0.0.1:9000
  Console:   http://127.0.0.1:9001  (minioadmin / minioadmin)
  Bucket:    openpuffer-dev

Start the server:
  ./scripts/dev-serve.sh

Stop MinIO:
  docker compose down

EOF