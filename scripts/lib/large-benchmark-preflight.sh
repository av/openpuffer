# Shared preflight for large-tier ingest/bench (G3 AWS scale proof, G4 turbopuffer baseline).
# Source from ingest-large.sh, bench-large.sh, run-aws-large-benchmark.sh,
# run-tpuf-large-benchmark.sh — do not execute directly.
# See docs/BENCHMARKS.md § large-dataset runbook and docs/PLAN_LARGE_DATASET_BENCHMARK.md G3/G4.

large_preflight_root() {
  if [[ -n "${LARGE_PREFLIGHT_ROOT:-}" ]]; then
    echo "$LARGE_PREFLIGHT_ROOT"
    return 0
  fi
  local here
  here="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  echo "$here"
}

large_preflight_need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "preflight: missing required command: $1" >&2
    return 1
  }
}

large_preflight_toolchain() {
  large_preflight_need_cmd curl
  large_preflight_need_cmd jq
  large_preflight_need_cmd python3
}

large_preflight_ann_version() {
  local ann="${OPENPUFFER_ANN_VERSION:-3}"
  if [[ "$ann" != "3" ]]; then
    echo "preflight: warning OPENPUFFER_ANN_VERSION=${ann} (large-tier program expects 3)" >&2
  fi
  export OPENPUFFER_ANN_VERSION="$ann"
}

# Classify storage backend from endpoint URL (aws-s3 | minio | s3-compatible).
large_preflight_detect_environment() {
  local endpoint="${OPENPUFFER_S3_ENDPOINT:-}"
  if [[ -n "${OPENPUFFER_BENCH_ENVIRONMENT:-}" ]]; then
    echo "$OPENPUFFER_BENCH_ENVIRONMENT"
    return 0
  fi
  if [[ -n "${OPENPUFFER_INGEST_ENVIRONMENT:-}" ]]; then
    echo "$OPENPUFFER_INGEST_ENVIRONMENT"
    return 0
  fi
  local lower="${endpoint,,}"
  if [[ "$lower" == *amazonaws.com* ]]; then
    echo "aws-s3"
    return 0
  fi
  if [[ "$lower" == *minio* ]] \
    || [[ "$lower" == *127.0.0.1* ]] \
    || [[ "$lower" == *localhost* ]] \
    || [[ "$lower" == *:9000* ]]; then
    echo "minio"
    return 0
  fi
  echo "s3-compatible"
}

large_preflight_validate_s3_env() {
  : "${OPENPUFFER_S3_ENDPOINT:?preflight: set OPENPUFFER_S3_ENDPOINT (AWS: https://s3.<region>.amazonaws.com)}"
  : "${OPENPUFFER_S3_BUCKET:?preflight: set OPENPUFFER_S3_BUCKET}"
  : "${OPENPUFFER_S3_ACCESS_KEY:?preflight: set OPENPUFFER_S3_ACCESS_KEY}"
  : "${OPENPUFFER_S3_SECRET_KEY:?preflight: set OPENPUFFER_S3_SECRET_KEY}"
  if [[ -z "${OPENPUFFER_S3_REGION:-}" ]]; then
    echo "preflight: OPENPUFFER_S3_REGION unset (defaulting serve/aws CLI to us-east-1)" >&2
  fi
}

# Probe bucket with aws CLI when available, else anonymous-style HEAD via endpoint path-style.
large_preflight_s3_head_bucket() {
  local bucket="${OPENPUFFER_S3_BUCKET}"
  local region="${OPENPUFFER_S3_REGION:-us-east-1}"
  if command -v aws >/dev/null 2>&1; then
    if aws s3api head-bucket --bucket "$bucket" --region "$region" >/dev/null 2>&1; then
      echo "preflight: S3 head-bucket OK (${bucket}, region=${region})"
      return 0
    fi
    echo "preflight: aws s3api head-bucket failed for ${bucket} (region=${region}); check IAM/credentials" >&2
    return 1
  fi
  echo "preflight: aws CLI not installed — skipping head-bucket (install awscli for stronger preflight)" >&2
  return 0
}

large_preflight_validate_tier_workload() {
  local tier="$1"
  local root="$2"
  local workload
  case "$tier" in
    l1) workload="${root}/benchmarks/workloads/synthetic-128/l1-100k" ;;
    l2) workload="${root}/benchmarks/workloads/synthetic-128/l2-500k" ;;
    l3) workload="${root}/benchmarks/workloads/synthetic-128/l3-1m" ;;
    *)
      echo "preflight: unknown tier ${tier}" >&2
      return 1
      ;;
  esac
  if [[ ! -f "${workload}/manifest.json" || ! -f "${workload}/queries.json" ]]; then
    echo "preflight: missing manifest.json or queries.json under ${workload}" >&2
    return 1
  fi
  python3 -c "
import json, sys
m=json.load(open('${workload}/manifest.json'))
q=json.load(open('${workload}/queries.json'))
assert q.get('cold_query_protocol',{}).get('runs')==7, 'cold_query_protocol.runs must be 7'
assert 'recall_defaults' in q, 'recall_defaults required'
" || return 1
  echo "preflight: workload OK tier=${tier} dir=${workload}"
}

# Block writing comparison artifacts from MinIO unless explicitly allowed.
large_preflight_guard_aws_results_path() {
  local env="$1"
  local results_path="$2"
  if [[ "$env" == "aws-s3" ]]; then
    return 0
  fi
  if [[ "${OPENPUFFER_BENCH_ALLOW_MINIO_RESULTS:-}" == "1" ]]; then
    return 0
  fi
  if [[ "$results_path" == *minio* ]] || [[ "$results_path" == *example* ]] || [[ "$results_path" == *schema* ]]; then
    return 0
  fi
  echo "preflight: environment=${env} — refusing to write comparison path ${results_path}" >&2
  echo "  Set OPENPUFFER_BENCH_RESULTS to a *minio*/*example* path, or OPENPUFFER_BENCH_ALLOW_MINIO_RESULTS=1" >&2
  echo "  MinIO timings must not replace AWS in COMPARISON.md (G2/correctness only)." >&2
  return 1
}

large_preflight_run_g2_subset() {
  local root="$1"
  echo "preflight: G2 MinIO correctness subset (blocks AWS spend if red)…"
  (cd "$root" && cargo test --test synthetic_workload_gate -q)
  (cd "$root" && cargo test -F bench --test bench_cold bench_cold_10k_synthetic_128_workload_gate -q)
  echo "preflight: G2 subset OK (full MinIO: ./scripts/run-minio-correctness-gates.sh)"
}

large_preflight_print_aws_operator_env() {
  cat <<'EOF'
Required for live AWS G3 run (same host as openpuffer serve — typically EC2 in bucket region):
  export OPENPUFFER_S3_ENDPOINT=https://s3.<region>.amazonaws.com
  export OPENPUFFER_S3_BUCKET=openpuffer-bench-<account>-<region>
  export OPENPUFFER_S3_ACCESS_KEY=...
  export OPENPUFFER_S3_SECRET_KEY=...
  export OPENPUFFER_S3_REGION=<region>          # e.g. us-east-1
  export OPENPUFFER_ANN_VERSION=3
  export OPENPUFFER_COLD_S3_CONCURRENCY=32      # try 64 if RTT-bound

Optional (document in report / JSON notes):
  export OPENPUFFER_BENCH_HOST_LABEL=c6i.large@us-east-1a
  export OPENPUFFER_BENCH_CLIENT_MODE=localhost   # serve on same EC2 (recommended)

EC2: launch in same region as bucket; SSH tunnel if needed; no public ingress required.
After G2 green: ./scripts/run-aws-large-benchmark.sh --tier l1
EOF
}

large_preflight_validate_tpuf_env() {
  : "${TURBOPUFFER_API_KEY:?preflight: set TURBOPUFFER_API_KEY (see https://turbopuffer.com/docs/testing)}"
  if [[ -z "${TURBOPUFFER_REGION:-}" ]]; then
    echo "preflight: TURBOPUFFER_REGION unset (defaulting driver to aws-us-east-1)" >&2
    export TURBOPUFFER_REGION=aws-us-east-1
  fi
  if [[ -n "${OPENPUFFER_S3_REGION:-}" ]]; then
    local aws="${OPENPUFFER_S3_REGION,,}"
    local tpuf="${TURBOPUFFER_REGION,,}"
    if [[ "$tpuf" != *"${aws}"* ]] && [[ "$aws" != *"${tpuf#aws-}"* ]]; then
      echo "preflight: warning OPENPUFFER_S3_REGION=${OPENPUFFER_S3_REGION} may not align with TURBOPUFFER_REGION=${TURBOPUFFER_REGION}" >&2
      echo "  Run tpuf from the same region as the openpuffer AWS bench host (plan § fairness)." >&2
    fi
  fi
}

large_preflight_tpuf_python_deps() {
  if python3 -c "import turbopuffer" >/dev/null 2>&1; then
    echo "preflight: turbopuffer Python package OK"
    return 0
  fi
  local req="${1}/benchmarks/tpuf_driver/requirements.txt"
  if [[ -f "$req" ]]; then
    echo "preflight: installing turbopuffer driver deps from ${req}…" >&2
    python3 -m pip install -q -r "$req"
    python3 -c "import turbopuffer" >/dev/null 2>&1 || {
      echo "preflight: failed to import turbopuffer after pip install" >&2
      return 1
    }
    echo "preflight: turbopuffer Python package OK (installed)"
    return 0
  fi
  echo "preflight: missing turbopuffer package; pip install -r benchmarks/tpuf_driver/requirements.txt" >&2
  return 1
}

large_preflight_print_tpuf_operator_env() {
  cat <<'EOF'
Required for live G4 turbopuffer run (same region as openpuffer AWS bench host):
  export TURBOPUFFER_API_KEY=tpuf_...
  export TURBOPUFFER_REGION=aws-us-east-1    # align with OPENPUFFER_S3_REGION / EC2
  export TURBOPUFFER_BENCH_TIER=l1           # l1|l2|l3

Optional:
  export TURBOPUFFER_BENCH_NAMESPACE=bench-tpuf-YYYY-MM-DD-l1
  export TURBOPUFFER_BENCH_RESULTS=benchmarks/results/tpuf-l1.json
  export TURBOPUFFER_BENCH_ENFORCE_GATES=1   # recall@10 >= 0.85 + index up-to-date
  export TURBOPUFFER_BENCH_SKIP_DELETE=1     # keep namespace for debug

After G3 large-aws JSON (or in parallel on a host with API access):
  ./scripts/run-tpuf-large-benchmark.sh --tier l1
  ./scripts/run-tpuf-large-benchmark.sh --tier l1 --warm   # adds hint_cache_warm + eventual latencies
EOF
}