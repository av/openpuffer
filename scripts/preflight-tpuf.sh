#!/usr/bin/env bash
# turbopuffer managed API preflight for G4 large-dataset benchmarks.
# Validates API key presence, region alignment with AWS bench host, optional RTT,
# tier workload, cost guardrails, and artifact secret scan before commit.
#
# Usage:
#   ./scripts/preflight-tpuf.sh --tier l1
#   ./scripts/preflight-tpuf.sh --tier l3 --warm
#   ./scripts/preflight-tpuf.sh --tier l1 --warn-only     # region mismatch → warning
#   ./scripts/preflight-tpuf.sh --check-results benchmarks/results/tpuf-l1.json
#   ./scripts/preflight-tpuf.sh --skip-rtt                # offline key/region/workload only
#
# Environment:
#   TURBOPUFFER_API_KEY (required unless --check-results only)
#   TURBOPUFFER_REGION (default aws-us-east-1; should match OPENPUFFER_S3_REGION)
#   OPENPUFFER_S3_REGION (optional; used for region alignment)
#
# See docs/BENCHMARKS.md § G4 — turbopuffer operator setup.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export LARGE_PREFLIGHT_ROOT="$ROOT"
# shellcheck source=scripts/lib/large-benchmark-preflight.sh
source "$ROOT/scripts/lib/large-benchmark-preflight.sh"

TIER="${TURBOPUFFER_BENCH_TIER:-l1}"
WARN_ONLY=0
SKIP_RTT=0
WARM_MODE=0
CHECK_RESULTS=""
SKIP_KEY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier=*) TIER="${1#*=}"; shift ;;
    --tier)
      shift
      TIER="${1:?--tier requires l1|l2|l3}"
      shift
      ;;
    --warm) WARM_MODE=1; shift ;;
    --warn-only) WARN_ONLY=1; shift ;;
    --skip-rtt) SKIP_RTT=1; shift ;;
    --check-results)
      shift
      CHECK_RESULTS="${1:?--check-results requires a file path}"
      shift
      ;;
    --skip-key) SKIP_KEY=1; shift ;;
    -h|--help)
      sed -n '2,22p' "$0"
      large_preflight_print_tpuf_operator_env
      exit 0
      ;;
    *) large_benchmark_exit_usage "preflight-tpuf: unknown argument: $1" ;;
  esac
done

case "$TIER" in
  l1|l2|l3) ;;
  *)
    large_benchmark_exit_preflight "preflight-tpuf: unknown tier ${TIER} (use l1, l2, or l3)"
    ;;
esac

IMDS_BASE="http://169.254.169.254/latest"
IMDS_TOKEN=""
EC2_REGION=""

imds_token() {
  curl -sf -m 2 -X PUT "${IMDS_BASE}/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || return 1
}

imds_get() {
  local path="$1"
  curl -sf -m 2 -H "X-aws-ec2-metadata-token: ${IMDS_TOKEN}" "${IMDS_BASE}/${path}" 2>/dev/null
}

preflight_tpuf_ec2_region() {
  IMDS_TOKEN="$(imds_token || true)"
  if [[ -z "$IMDS_TOKEN" ]]; then
    return 0
  fi
  EC2_REGION="$(imds_get meta-data/placement/region 2>/dev/null || true)"
  if [[ -z "$EC2_REGION" ]]; then
    local az
    az="$(imds_get meta-data/placement/availability-zone || true)"
    [[ -n "$az" ]] && EC2_REGION="${az::-1}"
  fi
  if [[ -n "$EC2_REGION" ]]; then
    echo "preflight-tpuf: EC2 placement region=${EC2_REGION}"
  fi
}

preflight_tpuf_api_key() {
  [[ "$SKIP_KEY" == "1" || -n "$CHECK_RESULTS" ]] && return 0
  if [[ -z "${TURBOPUFFER_API_KEY:-}" ]]; then
    echo "preflight-tpuf: TURBOPUFFER_API_KEY unset (create key in dedicated test org per https://turbopuffer.com/docs/testing)" >&2
    return 1
  fi
  if [[ ! "${TURBOPUFFER_API_KEY}" =~ ^tpuf_ ]]; then
    echo "preflight-tpuf: warning TURBOPUFFER_API_KEY does not start with tpuf_ (verify org/key type)" >&2
  fi
  echo "preflight-tpuf: TURBOPUFFER_API_KEY=set"
}

preflight_tpuf_region() {
  local s3_region="${OPENPUFFER_S3_REGION:-us-east-1}"
  if [[ -z "${TURBOPUFFER_REGION:-}" ]]; then
    export TURBOPUFFER_REGION
    TURBOPUFFER_REGION="$(large_preflight_tpuf_region_for_s3 "$s3_region")"
    echo "preflight-tpuf: TURBOPUFFER_REGION unset → ${TURBOPUFFER_REGION} (from OPENPUFFER_S3_REGION=${s3_region})"
  fi

  if [[ -n "$EC2_REGION" ]]; then
    local expected_ec2
    expected_ec2="$(large_preflight_tpuf_region_for_s3 "$EC2_REGION")"
    if [[ "${TURBOPUFFER_REGION,,}" != "${expected_ec2,,}" ]]; then
      echo "preflight-tpuf: EC2 region (${EC2_REGION}) → tpuf ${expected_ec2} but TURBOPUFFER_REGION=${TURBOPUFFER_REGION}" >&2
      [[ "$WARN_ONLY" == "1" ]] || return 1
    else
      echo "preflight-tpuf: EC2 region matches TURBOPUFFER_REGION"
    fi
  fi

  if ! large_preflight_tpuf_regions_align; then
    [[ "$WARN_ONLY" == "1" ]] && return 0
    return 1
  fi
}

preflight_tpuf_rtt() {
  [[ "$SKIP_RTT" == "1" ]] && return 0
  large_preflight_need_cmd curl
  local region="${TURBOPUFFER_REGION:-aws-us-east-1}"
  local host="${region}.turbopuffer.com"
  local url="https://${host}/"
  local connect_ms
  connect_ms="$(curl -sf -m 5 -o /dev/null -w '%{time_connect}' "$url" 2>/dev/null || echo "")"
  if [[ -z "$connect_ms" ]]; then
    echo "preflight-tpuf: warning could not probe ${url} (network/DNS); continue if client is in-region" >&2
    return 0
  fi
  local connect_ms_int
  connect_ms_int="$(python3 -c "print(int(float('${connect_ms}') * 1000))")"
  echo "preflight-tpuf: curl connect to ${host}: ${connect_ms_int} ms"
  if [[ "$connect_ms_int" -gt 50 ]]; then
    echo "preflight-tpuf: warning connect >50ms — cold p50 deltas vs openpuffer may reflect RTT (use same-region EC2)" >&2
  fi
}

preflight_tpuf_testing_org() {
  cat <<'EOF'
preflight-tpuf: dedicated test org/namespace (https://turbopuffer.com/docs/testing):
  - Use a separate turbopuffer org from production; one API key per operator host (env only).
  - Namespace pattern: bench-tpuf-YYYY-MM-DD-{tier} (driver default) or TURBOPUFFER_BENCH_NAMESPACE.
  - Namespace create is cheap; always delete after the run unless debugging.
EOF
}

preflight_tpuf_delete_first() {
  local delete_first="${TURBOPUFFER_BENCH_DELETE_FIRST:-1}"
  if [[ "$delete_first" == "1" ]]; then
    echo "preflight-tpuf: TURBOPUFFER_BENCH_DELETE_FIRST=1 (delete namespace before ingest on re-runs)"
  else
    echo "preflight-tpuf: warning TURBOPUFFER_BENCH_DELETE_FIRST=0 — stale namespace may skew cold/recall" >&2
  fi
  if [[ "${TURBOPUFFER_BENCH_SKIP_DELETE:-}" == "1" ]]; then
    echo "preflight-tpuf: warning TURBOPUFFER_BENCH_SKIP_DELETE=1 — namespace left billed until manual delete_all" >&2
  else
    echo "preflight-tpuf: cleanup after run: driver delete_all in finally (unless SKIP_DELETE)"
  fi
}

main() {
  if [[ -n "$CHECK_RESULTS" ]]; then
    large_preflight_artifact_secret_scan "$CHECK_RESULTS"
    exit 0
  fi

  large_preflight_toolchain
  preflight_tpuf_ec2_region
  preflight_tpuf_api_key || large_benchmark_exit_preflight "preflight-tpuf: API key check failed"
  preflight_tpuf_region || large_benchmark_exit_preflight "preflight-tpuf: region alignment failed"
  large_preflight_validate_tier_workload "$TIER" "$ROOT"
  large_preflight_tpuf_python_deps "$ROOT"
  preflight_tpuf_testing_org
  preflight_tpuf_delete_first
  large_preflight_tpuf_cost_estimate "$TIER" "$WARM_MODE"
  preflight_tpuf_rtt
  echo "preflight-tpuf: OK (tier=${TIER} region=${TURBOPUFFER_REGION:-aws-us-east-1})"
}

main