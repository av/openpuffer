# shellcheck shell=bash
# Shared preflight for large-tier ingest/bench (G3 AWS scale proof, G4 turbopuffer baseline).
# Source from ingest-large.sh, bench-large.sh, run-aws-large-benchmark.sh,
# run-tpuf-large-benchmark.sh — do not execute directly.
# See docs/BENCHMARKS.md § large-dataset runbook and docs/PLAN_LARGE_DATASET_BENCHMARK.md G3/G4.

# shellcheck source=scripts/lib/large-benchmark-exit-codes.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/large-benchmark-exit-codes.sh"

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
  : "${OPENPUFFER_S3_BUCKET:?preflight: set OPENPUFFER_S3_BUCKET}"
  if [[ -z "${OPENPUFFER_S3_ENDPOINT:-}" ]]; then
    local r="${OPENPUFFER_S3_REGION:-us-east-1}"
    export OPENPUFFER_S3_ENDPOINT="https://s3.${r}.amazonaws.com"
    echo "preflight: OPENPUFFER_S3_ENDPOINT unset → ${OPENPUFFER_S3_ENDPOINT}" >&2
  fi
  if [[ -z "${OPENPUFFER_S3_ACCESS_KEY:-}" || -z "${OPENPUFFER_S3_SECRET_KEY:-}" ]]; then
    echo "preflight: OPENPUFFER_S3_ACCESS_KEY/SECRET unset — on EC2 run ./scripts/preflight-aws-ec2.sh first (instance profile)" >&2
    : "${OPENPUFFER_S3_ACCESS_KEY:?preflight: set OPENPUFFER_S3_ACCESS_KEY or use EC2 instance profile via preflight-aws-ec2.sh}"
    : "${OPENPUFFER_S3_SECRET_KEY:?preflight: set OPENPUFFER_S3_SECRET_KEY}"
  fi
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

# Default indexer poll timeout (seconds) when OPENPUFFER_*_INDEX_TIMEOUT_SEC is unset.
large_preflight_tier_index_timeout_sec() {
  local tier="$1"
  case "$tier" in
    l1) echo 7200 ;;   # 2h @ 100k
    l2) echo 10800 ;;  # 3h @ 500k
    l3) echo 14400 ;;  # 4h @ 1M
    *)
      echo "preflight: unknown tier ${tier} for index timeout" >&2
      return 1
      ;;
  esac
}

# Order-of-magnitude wall-clock hints for operators (m7i.xlarge, us-east-1; not a guarantee).
large_preflight_aws_time_estimate() {
  local tier="$1"
  case "$tier" in
    l1)
      cat <<'EOF'
preflight AWS time estimate (tier=l1, 100k docs, m7i.xlarge):
  ingest WAL commits (~10×10k @ 1.1s sleep): ~12–15 min
  index catch-up (poll index_cursor): often 15–60 min after ingest
  bench (cold+filter/hybrid+recall): ~5–15 min
  full G3 one-shot: ~30–90 min typical; allow OPENPUFFER_INGEST_INDEX_TIMEOUT_SEC=7200
EOF
      ;;
    l2)
      cat <<'EOF'
preflight AWS time estimate (tier=l2, 500k docs, m7i.xlarge):
  ingest WAL commits (~50×10k @ 1.1s sleep): ~60–70 min
  index catch-up: often 1–3 h (more S3 index objects than L1)
  bench: ~10–20 min (same query count as L1; recall num=20 bills ~100 tpuf-side)
  full G3 one-shot: ~2–4 h typical; default index timeout 10800s (override if indexer lags)
EOF
      ;;
    l3)
      cat <<'EOF'
preflight AWS time estimate (tier=l3, 1M docs, m7i.xlarge):
  ingest WAL commits (~100×10k @ 1.1s sleep): ~17–20 min (WAL-limited; not tpuf ingest time)
  index catch-up: often 2–4 h on single host (do not use c6i.large)
  bench: ~15–30 min; recall billed ~200 queries on tpuf if mirroring G4
  full G3 one-shot: ~3–6 h typical; default index timeout 14400s; run L1 green first
EOF
      ;;
    *)
      echo "preflight: unknown tier ${tier} for AWS time estimate" >&2
      return 1
      ;;
  esac
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
Required for live AWS G3 run (same host as openpuffer serve — EC2 in bucket region):
  export OPENPUFFER_S3_BUCKET=openpuffer-bench-<account>-<region>
  export OPENPUFFER_S3_REGION=<region>              # e.g. us-east-1
  export OPENPUFFER_S3_ENDPOINT=https://s3.<region>.amazonaws.com
  # Prefer EC2 instance profile (no long-lived keys):
  ./scripts/preflight-aws-ec2.sh                    # sets keys from role + head-bucket
  # Or export static keys only on the host (never commit):
  export OPENPUFFER_S3_ACCESS_KEY=...
  export OPENPUFFER_S3_SECRET_KEY=...
  export OPENPUFFER_ANN_VERSION=3
  export OPENPUFFER_COLD_S3_CONCURRENCY=32        # try 64 if RTT-bound

Optional (document in report / JSON notes):
  export OPENPUFFER_BENCH_HOST_LABEL=m7i.xlarge@us-east-1a
  export OPENPUFFER_BENCH_CLIENT_MODE=localhost     # serve on same EC2 (recommended)

EC2: m7i.xlarge (or c7i.xlarge) same AZ as bucket; IAM role on instance; no keys in git.
Full checklist: docs/BENCHMARKS.md § G3 — EC2 + AWS S3 operator setup
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
  local req="${1}/benchmarks/requirements.txt"
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
  echo "preflight: missing turbopuffer package; ./scripts/install-benchmark-python-deps.sh" >&2
  return 1
}

# Map AWS S3 region (e.g. us-east-1) → turbopuffer region id (e.g. aws-us-east-1).
large_preflight_tpuf_region_for_s3() {
  local aws="${1,,}"
  case "$aws" in
    us-east-1) echo "aws-us-east-1" ;;
    us-east-2) echo "aws-us-east-2" ;;
    us-west-2) echo "aws-us-west-2" ;;
    eu-west-1) echo "aws-eu-west-1" ;;
    eu-west-2) echo "aws-eu-west-2" ;;
    eu-central-1) echo "aws-eu-central-1" ;;
    ap-southeast-2) echo "aws-ap-southeast-2" ;;
    ap-south-1) echo "aws-ap-south-1" ;;
    ca-central-1) echo "aws-ca-central-1" ;;
    sa-east-1) echo "aws-sa-east-1" ;;
    gcp-us-central1|gcp-*) echo "$aws" ;;
    aws-*) echo "$aws" ;;
    *)
      echo "aws-${aws}"
      ;;
  esac
}

large_preflight_tpuf_regions_align() {
  local s3_region="${OPENPUFFER_S3_REGION:-}"
  local tpuf_region="${TURBOPUFFER_REGION:-}"
  if [[ -z "$s3_region" || -z "$tpuf_region" ]]; then
    return 0
  fi
  local expected
  expected="$(large_preflight_tpuf_region_for_s3 "$s3_region")"
  if [[ "${tpuf_region,,}" != "${expected,,}" ]]; then
    echo "preflight-tpuf: TURBOPUFFER_REGION=${tpuf_region} != expected ${expected} for OPENPUFFER_S3_REGION=${s3_region}" >&2
    return 1
  fi
  echo "preflight-tpuf: TURBOPUFFER_REGION aligns with OPENPUFFER_S3_REGION (${s3_region})"
  return 0
}

# Estimated API volume before a live G4 run (delegates to estimate-large-benchmark-cost.sh).
large_preflight_tpuf_cost_estimate() {
  local tier="$1"
  local warm="${2:-0}"
  local root
  root="$(large_preflight_root)"
  # shellcheck source=scripts/lib/estimate-large-benchmark-cost.sh
  source "${root}/scripts/lib/estimate-large-benchmark-cost.sh"
  large_benchmark_cost_print "$tier" "$warm" tpuf
}

# Fail if benchmark JSON still contains secret-like substrings (before git commit).
large_preflight_artifact_secret_scan() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "preflight-tpuf: artifact not found: ${path}" >&2
    return 1
  fi
  if grep -qE 'tpuf_[A-Za-z0-9_-]{8,}|TURBOPUFFER_API_KEY=|OPENPUFFER_S3_SECRET_KEY=' "$path" 2>/dev/null; then
    echo "preflight-tpuf: possible API key/secret in ${path} — scrub before commit (render-report redact patterns)" >&2
    return 1
  fi
  echo "preflight-tpuf: artifact secret scan OK (${path})"
  return 0
}

large_preflight_print_tpuf_operator_env() {
  cat <<'EOF'
Required for live G4 turbopuffer run (same region as openpuffer AWS bench host):
  export TURBOPUFFER_API_KEY=tpuf_...
  export TURBOPUFFER_REGION=aws-us-east-1    # align with OPENPUFFER_S3_REGION / EC2
  export TURBOPUFFER_BENCH_TIER=l1           # l1|l2|l3

Recommended guardrails:
  export TURBOPUFFER_BENCH_DELETE_FIRST=1    # delete namespace before ingest (re-runs)
  ./scripts/preflight-tpuf.sh --tier l1      # region RTT + cost estimate + key check

Optional:
  export TURBOPUFFER_BENCH_NAMESPACE=bench-tpuf-YYYY-MM-DD-l1
  export TURBOPUFFER_BENCH_RESULTS=benchmarks/results/tpuf-l1.json
  export TURBOPUFFER_BENCH_ENFORCE_GATES=1   # recall@10 >= 0.85 + index up-to-date
  export TURBOPUFFER_BENCH_SKIP_DELETE=1     # keep namespace after run (debug only; $)

After G3 large-aws JSON (or in parallel on same EC2 with API access):
  ./scripts/run-tpuf-large-benchmark.sh --tier l1
  ./scripts/run-tpuf-large-benchmark.sh --tier l1 --warm   # adds hint_cache_warm + eventual latencies
  ./scripts/preflight-tpuf.sh --check-results benchmarks/results/tpuf-l1.json
EOF
}