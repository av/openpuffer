#!/usr/bin/env bash
# End-to-end large-dataset comparison program (G2 → G3 → G4 → 3.3 → G5).
# After live G3/G4, runs check-large-aws-gates and check-tpuf-gates on result JSON.
# Chains operator wrappers; documents AWS + turbopuffer env in --help / --dry-run.
#
# Usage:
#   ./scripts/run-large-benchmark-program.sh --dry-run              # full plan, no spend
#   ./scripts/run-large-benchmark-program.sh --preflight-only       # G2 + env checks only
#   ./scripts/run-large-benchmark-program.sh --tier l1              # live when creds set
#   ./scripts/run-large-benchmark-program.sh --tier l1 --warm       # cold+filter/hybrid+warm
#   ./scripts/run-large-benchmark-program.sh --skip-g2              # skip MinIO G2 subset
#   ./scripts/run-large-benchmark-program.sh --full-g2              # run-minio-correctness-gates.sh
#   ./scripts/run-large-benchmark-program.sh --aws-only           # G3 only
#   ./scripts/run-large-benchmark-program.sh --tpuf-only --skip-g2  # G4 only (after G3 JSON)
#   ./scripts/run-large-benchmark-program.sh --skip-tpuf            # G3 + overlap + report
#   ./scripts/run-large-benchmark-program.sh --measured-report      # render-report without --dry-run
#
# Live prerequisites:
#   openpuffer: OPENPUFFER_S3_* on EC2 (see large_preflight_print_aws_operator_env)
#   turbopuffer: TURBOPUFFER_API_KEY + TURBOPUFFER_REGION aligned with S3 region
#   overlap: OPENPUFFER_BASE_URL (default http://127.0.0.1:3000) + both namespaces indexed
#
# See docs/BENCHMARKS.md § Large-dataset program — Operator runbook (Phases 4–6)
# and docs/PLAN_LARGE_DATASET_BENCHMARK.md Phase 8.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export LARGE_PREFLIGHT_ROOT="$ROOT"
# shellcheck source=scripts/lib/large-benchmark-preflight.sh
source "$ROOT/scripts/lib/large-benchmark-preflight.sh"
# shellcheck source=scripts/lib/tier-validate.sh
source "$ROOT/scripts/lib/tier-validate.sh"

TIER="${OPENPUFFER_BENCH_TIER:-l1}"
DRY_RUN=0
PREFLIGHT_ONLY=0
SKIP_G2=0
FULL_G2=0
WARM_MODE=0
AWS_ONLY=0
TPUF_ONLY=0
SKIP_TPUF=0
SKIP_OVERLAP=0
SKIP_REPORT=0
MEASURED_REPORT=0
REPORT_DATE="${OPENPUFFER_REPORT_DATE:-$(date -u +%F)}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run|-n) DRY_RUN=1 ;;
    --preflight-only) PREFLIGHT_ONLY=1 ;;
    --skip-g2) SKIP_G2=1 ;;
    --full-g2) FULL_G2=1 ;;
    --warm) WARM_MODE=1 ;;
    --aws-only) AWS_ONLY=1; SKIP_TPUF=1 ;;
    --tpuf-only) TPUF_ONLY=1 ;;
    --skip-tpuf) SKIP_TPUF=1 ;;
    --skip-overlap) SKIP_OVERLAP=1 ;;
    --skip-report) SKIP_REPORT=1 ;;
    --measured-report) MEASURED_REPORT=1 ;;
    --tier=*) TIER="${1#*=}" ;;
    --tier)
      shift
      TIER="${1:?--tier requires l1|l2|l3}"
      ;;
    --date=*) REPORT_DATE="${1#*=}" ;;
    --date)
      shift
      REPORT_DATE="${1:?--date requires YYYY-MM-DD}"
      ;;
    -h|--help)
      sed -n '2,28p' "$0"
      echo ""
      echo "=== openpuffer (G3) ==="
      large_preflight_print_aws_operator_env
      echo ""
      echo "=== turbopuffer (G4) ==="
      large_preflight_print_tpuf_operator_env
      echo ""
      echo "=== id overlap (Phase 3.3) ==="
      cat <<'EOF'
  export OPENPUFFER_BASE_URL=http://127.0.0.1:3000
  export OPENPUFFER_BENCH_NAMESPACE=bench-large-100000   # ingest-large default for l1
  export TURBOPUFFER_BENCH_NAMESPACE=bench-tpuf-YYYY-MM-DD-l1
  ./scripts/run-id-overlap-spotcheck.sh --tier l1
EOF
      exit 0
      ;;
    *)
      echo "unknown argument: $1 (try --help)" >&2
      exit 1
      ;;
  esac
  shift
done

validate_tier "$TIER"

OP_RESULTS="${OPENPUFFER_BENCH_RESULTS:-$ROOT/benchmarks/results/large-aws-${TIER}.json}"
TPUF_RESULTS="${TURBOPUFFER_BENCH_RESULTS:-$ROOT/benchmarks/results/tpuf-${TIER}.json}"
OVERLAP_RESULTS="${OPENPUFFER_ID_OVERLAP_RESULTS:-$ROOT/benchmarks/results/id-overlap-${TIER}.json}"
REPORT_OUT="${OPENPUFFER_REPORT_OUTPUT:-$ROOT/docs/reports/BENCHMARK_VS_TURBOPUFFER_${REPORT_DATE}.md}"

print_program_plan() {
  local g2_mode
  if [[ "$SKIP_G2" == 1 ]]; then
    g2_mode=skip
  elif [[ "$FULL_G2" == 1 ]]; then
    g2_mode=full-minio
  else
    g2_mode=subset
  fi
  echo "run-large-benchmark-program plan"
  echo "  tier=${TIER} dry_run=${DRY_RUN} warm=${WARM_MODE}"
  echo "  aws_only=${AWS_ONLY} tpuf_only=${TPUF_ONLY} skip_tpuf=${SKIP_TPUF}"
  echo "  g2: ${g2_mode}"
  echo "  artifacts:"
  echo "    openpuffer: ${OP_RESULTS}"
  echo "    turbopuffer: ${TPUF_RESULTS}"
  echo "    overlap: ${OVERLAP_RESULTS}"
  echo "    report: ${REPORT_OUT} (measured=${MEASURED_REPORT})"
  echo "  post-live SLO gates (when JSON exists):"
  echo "    check-large-aws-gates: ${OP_RESULTS}"
  echo "    check-tpuf-gates: ${TPUF_RESULTS}"
  echo ""
  echo "=== openpuffer (G3) ==="
  large_preflight_print_aws_operator_env
  echo ""
  echo "=== turbopuffer (G4) ==="
  large_preflight_print_tpuf_operator_env
  if [[ -n "${OPENPUFFER_S3_BUCKET:-}" ]]; then
    echo ""
    echo "  detected_storage=$(large_preflight_detect_environment)"
  fi
  if [[ -n "${TURBOPUFFER_API_KEY:-}" ]]; then
    echo "  TURBOPUFFER_API_KEY=set"
  else
    echo "  TURBOPUFFER_API_KEY unset (required for G4 live)"
  fi
  echo ""
  large_preflight_aws_time_estimate "$TIER"
  large_preflight_tpuf_cost_estimate "$TIER" "$WARM_MODE"
}

run_g2() {
  if [[ "$SKIP_G2" == "1" ]]; then
    echo "program: skipping G2"
    return 0
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    if [[ "$FULL_G2" == "1" ]]; then
      echo "==> G2 (dry-run) would run: ./scripts/run-minio-correctness-gates.sh"
    else
      echo "==> G2 (dry-run) would run: large_preflight_run_g2_subset (cargo test subset)"
    fi
    return 0
  fi
  if [[ "$FULL_G2" == "1" ]]; then
    echo "==> G2 full MinIO correctness gates"
    ./scripts/run-minio-correctness-gates.sh
    return 0
  fi
  large_preflight_run_g2_subset "$ROOT"
}

run_aws_phase() {
  local aws_args=(--tier "$TIER")
  [[ "$SKIP_G2" != "1" ]] && aws_args+=(--skip-g2)
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "==> G3 run-aws-large-benchmark --dry-run"
    ./scripts/run-aws-large-benchmark.sh "${aws_args[@]}" --dry-run
    return 0
  fi
  if [[ "$WARM_MODE" == "1" ]]; then
    echo "==> G3 ingest-large --tier ${TIER}"
    ./scripts/ingest-large.sh --tier "$TIER"
    echo "==> G3 bench-large --tier ${TIER} --warm"
    export OPENPUFFER_BENCH_RESULTS="$OP_RESULTS"
    export OPENPUFFER_BENCH_ENFORCE_GATES="${OPENPUFFER_BENCH_ENFORCE_GATES:-1}"
    ./scripts/bench-large.sh --tier "$TIER" --warm
    return 0
  fi
  echo "==> G3 run-aws-large-benchmark ${aws_args[*]}"
  export OPENPUFFER_BENCH_RESULTS="$OP_RESULTS"
  ./scripts/run-aws-large-benchmark.sh "${aws_args[@]}"
}

run_tpuf_phase() {
  local tpuf_args=(--tier "$TIER")
  [[ "$SKIP_G2" != "1" ]] && tpuf_args+=(--skip-g2)
  [[ "$WARM_MODE" == "1" ]] && tpuf_args+=(--warm)
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "==> G4 run-tpuf-large-benchmark --dry-run"
    ./scripts/run-tpuf-large-benchmark.sh "${tpuf_args[@]}" --dry-run
    return 0
  fi
  echo "==> G4 run-tpuf-large-benchmark ${tpuf_args[*]}"
  export TURBOPUFFER_BENCH_RESULTS="$TPUF_RESULTS"
  ./scripts/run-tpuf-large-benchmark.sh "${tpuf_args[@]}"
}

run_aws_gates() {
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "==> G3 check-large-aws-gates (dry-run) would run: ${OP_RESULTS}"
    return 0
  fi
  if [[ ! -f "$OP_RESULTS" ]]; then
    echo "program: skip check-large-aws-gates (missing ${OP_RESULTS})" >&2
    return 0
  fi
  echo "==> G3 check-large-aws-gates ${OP_RESULTS}"
  export OPENPUFFER_BENCH_ENFORCE_GATES="${OPENPUFFER_BENCH_ENFORCE_GATES:-1}"
  ./scripts/check-large-aws-gates.sh --tier "$TIER" "$OP_RESULTS"
}

run_tpuf_gates() {
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "==> G4 check-tpuf-gates (dry-run) would run: ${TPUF_RESULTS}"
    return 0
  fi
  if [[ ! -f "$TPUF_RESULTS" ]]; then
    echo "program: skip check-tpuf-gates (missing ${TPUF_RESULTS})" >&2
    return 0
  fi
  echo "==> G4 check-tpuf-gates ${TPUF_RESULTS}"
  export TURBOPUFFER_BENCH_ENFORCE_GATES="${TURBOPUFFER_BENCH_ENFORCE_GATES:-1}"
  ./scripts/check-tpuf-gates.sh --tier "$TIER" "$TPUF_RESULTS"
}

run_overlap_phase() {
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "==> Phase 3.3 id-overlap --dry-run"
    ./scripts/run-id-overlap-spotcheck.sh --tier "$TIER" --dry-run
    return 0
  fi
  echo "==> Phase 3.3 id-overlap (live)"
  export OPENPUFFER_ID_OVERLAP_RESULTS="$OVERLAP_RESULTS"
  ./scripts/run-id-overlap-spotcheck.sh --tier "$TIER"
}

run_report_phase() {
  local report_args=(
    --tier "$TIER"
    --date "$REPORT_DATE"
    --output "$REPORT_OUT"
    --openpuffer-json "$OP_RESULTS"
    --tpuf-json "$TPUF_RESULTS"
    --overlap-json "$OVERLAP_RESULTS"
  )
  if [[ "$MEASURED_REPORT" != "1" ]]; then
    report_args+=(--dry-run)
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    report_args=(--dry-run --tier "$TIER" --date "$REPORT_DATE")
    echo "==> G5 render-report ${report_args[*]} (fixtures)"
    ./scripts/render-report.sh "${report_args[@]}"
    return 0
  fi
  echo "==> G5 render-report ${report_args[*]}"
  ./scripts/render-report.sh "${report_args[@]}"
}

[[ "$DRY_RUN" == "1" ]] && print_program_plan

large_preflight_toolchain
large_preflight_ann_version
large_preflight_validate_tier_workload "$TIER" "$ROOT"

if [[ "$DRY_RUN" == "1" ]]; then
  run_g2
  [[ "$TPUF_ONLY" != "1" ]] && run_aws_phase
  [[ "$TPUF_ONLY" != "1" ]] && run_aws_gates
  [[ "$AWS_ONLY" != "1" && "$SKIP_TPUF" != "1" ]] && run_tpuf_phase
  [[ "$AWS_ONLY" != "1" && "$SKIP_TPUF" != "1" ]] && run_tpuf_gates
  [[ "$SKIP_OVERLAP" != "1" ]] && run_overlap_phase
  [[ "$SKIP_REPORT" != "1" ]] && run_report_phase
  echo "run-large-benchmark-program dry-run: OK"
  exit 0
fi

run_g2

if [[ "$PREFLIGHT_ONLY" == "1" ]]; then
  if [[ "$TPUF_ONLY" != "1" ]]; then
    ./scripts/run-aws-large-benchmark.sh --tier "$TIER" --preflight-only --skip-g2
  fi
  if [[ "$AWS_ONLY" != "1" && "$SKIP_TPUF" != "1" ]]; then
    ./scripts/run-tpuf-large-benchmark.sh --tier "$TIER" --preflight-only --skip-g2
  fi
  echo "preflight-only: OK (G2 + per-phase preflight). Re-run without --preflight-only for live program."
  exit 0
fi

if [[ "$TPUF_ONLY" != "1" ]]; then
  run_aws_phase
  run_aws_gates
fi

if [[ "$AWS_ONLY" != "1" && "$SKIP_TPUF" != "1" ]]; then
  run_tpuf_phase
  run_tpuf_gates
fi

if [[ "$SKIP_OVERLAP" != "1" ]]; then
  run_overlap_phase
fi

if [[ "$SKIP_REPORT" != "1" ]]; then
  run_report_phase
fi

echo "run-large-benchmark-program complete (tier=${TIER})"
echo "  openpuffer: ${OP_RESULTS}"
[[ "$SKIP_TPUF" != "1" ]] && echo "  turbopuffer: ${TPUF_RESULTS}"
[[ "$SKIP_OVERLAP" != "1" ]] && echo "  overlap: ${OVERLAP_RESULTS}"
[[ "$SKIP_REPORT" != "1" ]] && echo "  report: ${REPORT_OUT}"
if [[ "$MEASURED_REPORT" != "1" ]]; then
  echo "  (report used --dry-run fixtures; re-run with --measured-report when JSON is committed)"
fi