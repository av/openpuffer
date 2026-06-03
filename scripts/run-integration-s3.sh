#!/usr/bin/env bash
# Run openpuffer integration tests against real S3-compatible storage.
#
#   ./scripts/run-integration-s3.sh           # MinIO via testcontainers (default)
#   ./scripts/run-integration-s3.sh external    # compose MinIO on :9000 + ignored external tests
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE_FILE="docker-compose.test.yml"
TEST_ENDPOINT="${OPENPUFFER_TEST_S3_ENDPOINT:-http://127.0.0.1:9000}"
TEST_BUCKET="${OPENPUFFER_TEST_S3_BUCKET:-openpuffer-integration}"
TEST_ACCESS_KEY="${OPENPUFFER_TEST_S3_ACCESS_KEY:-minioadmin}"
TEST_SECRET_KEY="${OPENPUFFER_TEST_S3_SECRET_KEY:-minioadmin}"

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

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "error: docker is required" >&2
    exit 1
  fi
  if ! docker info >/dev/null 2>&1; then
    echo "error: docker daemon is not reachable" >&2
    exit 1
  fi
}

wait_for_minio() {
  echo "Waiting for MinIO at ${TEST_ENDPOINT}..."
  for _ in $(seq 1 60); do
    if curl -sf "${TEST_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "error: MinIO did not become healthy at ${TEST_ENDPOINT}" >&2
  exit 1
}

ensure_compose_minio() {
  require_docker
  if curl -sf "${TEST_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
    echo "MinIO already reachable at ${TEST_ENDPOINT} (skipping compose up)"
    return 0
  fi
  echo "Starting test MinIO (${COMPOSE_FILE})..."
  compose up -d
  wait_for_minio
  compose run --rm minio-init >/dev/null
}

print_external_env_docs() {
  cat <<EOF

External S3 env (used by integration_external_s3 and manual serve):

  export OPENPUFFER_TEST_S3_ENDPOINT=${TEST_ENDPOINT}
  export OPENPUFFER_TEST_S3_BUCKET=${TEST_BUCKET}
  export OPENPUFFER_TEST_S3_ACCESS_KEY=${TEST_ACCESS_KEY}
  export OPENPUFFER_TEST_S3_SECRET_KEY=${TEST_SECRET_KEY}

  cargo test -F integration --test integration_external_s3 -- --ignored

Manual server against the same bucket:

  export OPENPUFFER_S3_ENDPOINT=${TEST_ENDPOINT}
  export OPENPUFFER_S3_BUCKET=${TEST_BUCKET}
  export OPENPUFFER_S3_ACCESS_KEY=${TEST_ACCESS_KEY}
  export OPENPUFFER_S3_SECRET_KEY=${TEST_SECRET_KEY}
  ./scripts/dev-serve.sh

Stop compose test MinIO:
  docker compose -f ${COMPOSE_FILE} down

EOF
}

run_testcontainers() {
  require_docker
  echo "Building openpuffer with integration feature..."
  cargo build --features integration

  echo "Running integration tests (MinIO testcontainers)..."
  cargo test -F integration

  export OPENPUFFER_TEST_S3_ENDPOINT="${TEST_ENDPOINT}"
  export OPENPUFFER_TEST_S3_BUCKET="${TEST_BUCKET}"
  export OPENPUFFER_TEST_S3_ACCESS_KEY="${TEST_ACCESS_KEY}"
  export OPENPUFFER_TEST_S3_SECRET_KEY="${TEST_SECRET_KEY}"
  print_external_env_docs
}

run_external() {
  ensure_compose_minio

  export OPENPUFFER_TEST_S3_ENDPOINT="${TEST_ENDPOINT}"
  export OPENPUFFER_TEST_S3_BUCKET="${TEST_BUCKET}"
  export OPENPUFFER_TEST_S3_ACCESS_KEY="${TEST_ACCESS_KEY}"
  export OPENPUFFER_TEST_S3_SECRET_KEY="${TEST_SECRET_KEY}"

  echo "Building openpuffer with integration feature..."
  cargo build --features integration

  echo "Running external S3 integration tests (${TEST_ENDPOINT}, bucket=${TEST_BUCKET})..."
  cargo test -F integration --test integration_external_s3 -- --ignored

  print_external_env_docs
}

case "${1:-}" in
  "" | testcontainers)
    run_testcontainers
    ;;
  external | compose)
    run_external
    ;;
  -h | --help | help)
    cat <<'EOF'
Usage:
  ./scripts/run-integration-s3.sh              # testcontainers (default)
  ./scripts/run-integration-s3.sh external     # docker-compose.test.yml MinIO + ignored tests

Environment (external mode, defaults shown):
  OPENPUFFER_TEST_S3_ENDPOINT=http://127.0.0.1:9000
  OPENPUFFER_TEST_S3_BUCKET=openpuffer-integration
  OPENPUFFER_TEST_S3_ACCESS_KEY=minioadmin
  OPENPUFFER_TEST_S3_SECRET_KEY=minioadmin
EOF
    ;;
  *)
    echo "error: unknown argument: $1 (try --help)" >&2
    exit 1
    ;;
esac