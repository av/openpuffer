#!/usr/bin/env bash
# EC2 + AWS S3 preflight for G3 large-dataset benchmarks.
# Validates IMDS (when on EC2), region/AZ alignment, optional instance-profile creds,
# and S3 head-bucket before ingest/bench spend.
#
# Usage:
#   ./scripts/preflight-aws-ec2.sh              # checks only (exit codes: benchmarks/README.md)
#   ./scripts/preflight-aws-ec2.sh --tier l2    # cost estimate for tier
#   ./scripts/preflight-aws-ec2.sh --dry-run    # cost estimate only (no S3/IMDS checks)
#   ./scripts/preflight-aws-ec2.sh --export-creds  # write exports to a chmod-600 temp file; prints path only
#   source "$(./scripts/preflight-aws-ec2.sh --export-creds)"  # populate OPENPUFFER_S3_* keys (no secrets on stdout)
#
# Environment (required for S3 checks unless --skip-s3):
#   OPENPUFFER_S3_BUCKET, OPENPUFFER_S3_REGION (default us-east-1)
#   OPENPUFFER_S3_ENDPOINT (optional; derived from region when unset)
#
# See docs/BENCHMARKS.md § G3 — EC2 + AWS S3 operator setup.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export LARGE_PREFLIGHT_ROOT="$ROOT"
# shellcheck source=scripts/lib/large-benchmark-preflight.sh
source "$ROOT/scripts/lib/large-benchmark-preflight.sh"
# shellcheck source=scripts/lib/estimate-large-benchmark-cost.sh
source "$ROOT/scripts/lib/estimate-large-benchmark-cost.sh"

EXPORT_CREDS=0
SKIP_S3=0
WARN_ONLY=0
DRY_RUN=0
TIER="${OPENPUFFER_BENCH_TIER:-l1}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --export-creds) EXPORT_CREDS=1; shift ;;
    --skip-s3) SKIP_S3=1; shift ;;
    --warn-only) WARN_ONLY=1; shift ;;
    --dry-run|-n) DRY_RUN=1; shift ;;
    --tier=*) TIER="${1#*=}"; shift ;;
    --tier)
      shift
      TIER="${1:?--tier requires l1|l2|l3}"
      shift
      ;;
    -h|--help)
      sed -n '2,18p' "$0"
      exit 0
      ;;
    *) large_benchmark_exit_usage "preflight-aws-ec2: unknown argument: $1" ;;
  esac
done

case "$TIER" in
  l1|l2|l3) ;;
  *)
    large_benchmark_exit_preflight "preflight-aws-ec2: unknown tier ${TIER} (use l1, l2, or l3)"
    ;;
esac

IMDS_BASE="http://169.254.169.254/latest"
IMDS_TOKEN=""
ON_EC2=0
EC2_INSTANCE_ID=""
EC2_INSTANCE_TYPE=""
EC2_AZ=""
EC2_REGION=""

imds_token() {
  curl -sf -m 2 -X PUT "${IMDS_BASE}/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || return 1
}

imds_get() {
  local path="$1"
  curl -sf -m 2 -H "X-aws-ec2-metadata-token: ${IMDS_TOKEN}" "${IMDS_BASE}/${path}" 2>/dev/null
}

preflight_ec2_detect() {
  IMDS_TOKEN="$(imds_token || true)"
  if [[ -z "$IMDS_TOKEN" ]]; then
    echo "preflight-ec2: not on EC2 (IMDSv2 unavailable); skipping instance metadata checks"
    return 0
  fi
  EC2_INSTANCE_ID="$(imds_get meta-data/instance-id || true)"
  EC2_INSTANCE_TYPE="$(imds_get meta-data/instance-type || true)"
  EC2_AZ="$(imds_get meta-data/placement/availability-zone || true)"
  EC2_REGION="$(imds_get meta-data/placement/region 2>/dev/null || true)"
  if [[ -z "$EC2_REGION" && -n "$EC2_AZ" ]]; then
    EC2_REGION="${EC2_AZ::-1}"
  fi
  if [[ -n "$EC2_INSTANCE_ID" ]]; then
    ON_EC2=1
    echo "preflight-ec2: instance-id=${EC2_INSTANCE_ID} type=${EC2_INSTANCE_TYPE} az=${EC2_AZ} region=${EC2_REGION}"
  fi
}

preflight_ec2_recommend_instance() {
  if [[ "$ON_EC2" != "1" ]]; then
    return 0
  fi
  local t="${EC2_INSTANCE_TYPE,,}"
  case "$t" in
    m7i.xlarge|m7i.2xlarge|m7i.large|c7i.xlarge|c6i.xlarge|c6i.large)
      echo "preflight-ec2: instance type ${EC2_INSTANCE_TYPE} OK for L1–L3 bench (CPU + memory for v3 index build)"
      ;;
    m7i.*|c7i.*|c6i.*|m6i.*|c6a.*)
      echo "preflight-ec2: instance type ${EC2_INSTANCE_TYPE} acceptable; prefer m7i.xlarge for stable L1 index wait"
      ;;
    t3.*|t2.*|t4g.*)
      echo "preflight-ec2: warning ${EC2_INSTANCE_TYPE} is burstable — index catch-up may lag vs compute-optimized families" >&2
      ;;
    *)
      echo "preflight-ec2: warning unfamiliar instance type ${EC2_INSTANCE_TYPE}; document in OPENPUFFER_BENCH_HOST_LABEL" >&2
      ;;
  esac
}

preflight_ec2_region_match() {
  local s3_region="${OPENPUFFER_S3_REGION:-us-east-1}"
  if [[ "$ON_EC2" != "1" || -z "$EC2_REGION" ]]; then
    return 0
  fi
  if [[ "$EC2_REGION" != "$s3_region" ]]; then
    echo "preflight-ec2: EC2 region (${EC2_REGION}) != OPENPUFFER_S3_REGION (${s3_region}) — cold p50 and index lag will skew" >&2
    [[ "$WARN_ONLY" == "1" ]] && return 0
    return 1
  fi
  echo "preflight-ec2: EC2 region matches OPENPUFFER_S3_REGION=${s3_region}"
}

preflight_ec2_default_endpoint() {
  local s3_region="${OPENPUFFER_S3_REGION:-us-east-1}"
  if [[ -z "${OPENPUFFER_S3_ENDPOINT:-}" ]]; then
    export OPENPUFFER_S3_ENDPOINT="https://s3.${s3_region}.amazonaws.com"
    echo "preflight-ec2: OPENPUFFER_S3_ENDPOINT unset → ${OPENPUFFER_S3_ENDPOINT}"
  fi
}

preflight_ec2_host_label() {
  if [[ -n "${OPENPUFFER_BENCH_HOST_LABEL:-}" ]]; then
    return 0
  fi
  if [[ "$ON_EC2" == "1" && -n "$EC2_INSTANCE_TYPE" && -n "$EC2_AZ" ]]; then
    export OPENPUFFER_BENCH_HOST_LABEL="${EC2_INSTANCE_TYPE}@${EC2_AZ}"
    echo "preflight-ec2: OPENPUFFER_BENCH_HOST_LABEL=${OPENPUFFER_BENCH_HOST_LABEL}"
  fi
}

preflight_ec2_client_mode() {
  if [[ -z "${OPENPUFFER_BENCH_CLIENT_MODE:-}" ]]; then
    export OPENPUFFER_BENCH_CLIENT_MODE=localhost
    echo "preflight-ec2: OPENPUFFER_BENCH_CLIENT_MODE=localhost (serve on same host as bench client)"
  fi
}

# Export temporary keys from instance profile for openpuffer serve (static keys required today).
preflight_ec2_export_role_creds() {
  if [[ -n "${OPENPUFFER_S3_ACCESS_KEY:-}" && -n "${OPENPUFFER_S3_SECRET_KEY:-}" ]]; then
    [[ "$EXPORT_CREDS" == "1" ]] && return 0
    return 0
  fi
  if [[ "$ON_EC2" != "1" ]]; then
    return 0
  fi
  if ! command -v aws >/dev/null 2>&1; then
    echo "preflight-ec2: OPENPUFFER_S3_* keys unset and aws CLI missing (install awscli or set keys)" >&2
    return 1
  fi
  local json
  json="$(aws configure export-credentials --format process 2>/dev/null || true)"
  if [[ -z "$json" ]]; then
    echo "preflight-ec2: could not export instance-profile credentials (attach IAM role to EC2)" >&2
    return 1
  fi
  local ak sk tok
  ak="$(echo "$json" | jq -r '.AccessKeyId // empty')"
  sk="$(echo "$json" | jq -r '.SecretAccessKey // empty')"
  tok="$(echo "$json" | jq -r '.SessionToken // empty')"
  if [[ -z "$ak" || -z "$sk" ]]; then
    echo "preflight-ec2: instance profile export missing AccessKeyId/SecretAccessKey" >&2
    return 1
  fi
  if [[ "$EXPORT_CREDS" == "1" ]]; then
    local credfile
    credfile="$(mktemp -t openpuffer-ec2-creds.XXXXXX)"
    chmod 600 "$credfile"
    {
      printf "export OPENPUFFER_S3_ACCESS_KEY='%s'\n" "$ak"
      printf "export OPENPUFFER_S3_SECRET_KEY='%s'\n" "$sk"
      if [[ -n "$tok" && "$tok" != "null" ]]; then
        printf "export AWS_SESSION_TOKEN='%s'\n" "$tok"
        echo "# openpuffer serve uses static key pair; session token is for aws CLI only"
      fi
    } >"$credfile"
    echo "preflight-ec2: instance-profile keys in ${credfile} (mode 600); rm after use" >&2
    echo "$credfile"
    return 0
  fi
  export OPENPUFFER_S3_ACCESS_KEY="$ak"
  export OPENPUFFER_S3_SECRET_KEY="$sk"
  echo "preflight-ec2: populated OPENPUFFER_S3_* from instance profile (session keys; rotate automatically)"
}

preflight_ec2_s3() {
  [[ "$SKIP_S3" == "1" ]] && return 0
  : "${OPENPUFFER_S3_BUCKET:?preflight-ec2: set OPENPUFFER_S3_BUCKET}"
  preflight_ec2_default_endpoint
  large_preflight_s3_head_bucket
}

preflight_ec2_cost_estimate() {
  large_benchmark_cost_print "$TIER" 0 aws
}

main() {
  if [[ "$DRY_RUN" == "1" ]]; then
    large_preflight_need_cmd python3
    preflight_ec2_cost_estimate
    echo "preflight-aws-ec2 dry-run OK (tier=${TIER}; no IMDS/S3 checks)"
    exit 0
  fi
  large_preflight_need_cmd curl
  large_preflight_need_cmd jq
  preflight_ec2_detect
  preflight_ec2_recommend_instance
  preflight_ec2_region_match || large_benchmark_exit_preflight "preflight-aws-ec2: EC2/S3 region mismatch"
  preflight_ec2_host_label
  preflight_ec2_client_mode
  if [[ "$EXPORT_CREDS" == "1" ]]; then
    preflight_ec2_export_role_creds || large_benchmark_exit_preflight "preflight-aws-ec2: could not export instance-profile credentials"
    exit 0
  fi
  preflight_ec2_export_role_creds || true
  preflight_ec2_s3 || large_benchmark_exit_preflight "preflight-aws-ec2: S3 head-bucket or bucket env failed"
  preflight_ec2_cost_estimate
  echo "preflight-ec2: OK (tier=${TIER})"
}

main