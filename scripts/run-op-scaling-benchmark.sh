#!/usr/bin/env bash
# Unified openpuffer MinIO scaling benchmarks (release + OPENPUFFER_ANN_VERSION=3).
#
# Runs 10k / 50k / 100k cold and 10k / 100k warm; writes benchmarks/results/op-scaling-*.json.
#
# Post-run validation (validate-benchmark-json.sh) gates op-scaling-100k.json cold p50:
#   - FAIL if p50 > 2000 ms (likely resource contention — re-run on a quiet host)
#   - WARN if p50 < 200 ms (suspiciously fast — check cache/warm contamination)
#
# Usage:
#   ./scripts/run-op-scaling-benchmark.sh           # all tiers
#   ./scripts/run-op-scaling-benchmark.sh 10k warm       # subset
#   ./scripts/run-op-scaling-benchmark.sh 100k-warm      # warm @ 100k (ingest+index ~3–8 min)
#   ./scripts/run-op-scaling-benchmark.sh --dry-run      # plan only (no cargo/docker)
#
set -euo pipefail

usage() {
  cat <<'EOF'
run-op-scaling-benchmark.sh — regenerate openpuffer MinIO scaling tiers (release + ANN v3).

Purpose:
  Run cold/warm doc-count benchmarks (10k / 50k / 100k × 128-d) via cargo integration
  tests; write benchmarks/results/op-scaling-*.json for comparison vs turbopuffer.

Usage:
  ./scripts/run-op-scaling-benchmark.sh              # all tiers (~1–3 h)
  ./scripts/run-op-scaling-benchmark.sh 10k warm     # subset
  ./scripts/run-op-scaling-benchmark.sh 100k-warm      # warm @ 100k (~3–8 min ingest+index)
  ./scripts/run-op-scaling-benchmark.sh --dry-run        # plan tiers + est wall; no cargo/docker
  ./scripts/run-op-scaling-benchmark.sh -h|--help

Environment (set by this script):
  OPENPUFFER_ANN_VERSION=3
  RUST_BACKTRACE=1

Prerequisites:
  Docker (MinIO testcontainers), Rust release build, integration + bench features.

Tiers (optional args; default = all):
  10k           cold query @ 10k docs
  50k           cold @ 50k (stress_50k, --ignored)
  100k          cold @ 100k (ingest+index ~8–15 min)
  warm          10k warm path
  100k-warm     100k warm path (~3–8 min)
  synthetic128  10k synthetic-128 workload gate

Output files (under benchmarks/results/):
  op-scaling-10k.json
  op-scaling-50k.json
  op-scaling-100k.json
  op-scaling-10k-warm.json
  op-scaling-100k-warm.json
  op-scaling-10k-synthetic128.json

Post-run: validate-benchmark-json.sh on each op-scaling-*.json (100k cold p50 gate).

Quickstart:
  benchmarks/SCALING_VS_TPUF_QUICKSTART.md  (make bench-op-scaling → bench-compare-tpuf)
EOF
}

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DRY_RUN=0
TIER_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --dry-run|-n) DRY_RUN=1 ;;
    -h|--help)
      usage
      exit 0
      ;;
    *) TIER_ARGS+=("$arg") ;;
  esac
done
set -- "${TIER_ARGS[@]}"

# Wall-time hints from benchmarks/CHANGELOG_LARGE_DATASET.md (ingest) + harness comments.
tier_est_wall_note() {
  case "$1" in
    10k)           echo "~3–5 min tier (CHANGELOG ingest ~11 s @ 909 docs/s; + cold query)" ;;
    50k)           echo "~5–12 min tier (CHANGELOG ingest ~14 s @ 3571 docs/s; stress_50k)" ;;
    100k)          echo "~8–15 min tier (CHANGELOG ingest ~132 s @ 758 docs/s; nightly cold)" ;;
    warm)          echo "~2–4 min tier (10k warm path; no 100k ingest)" ;;
    100k-warm)     echo "~3–8 min tier (CHANGELOG-class ingest ~136 s + warm @ 100k)" ;;
    synthetic128)  echo "~3–5 min tier (10k synthetic-128 G2 gate)" ;;
    *)             echo "unknown tier" ;;
  esac
}

tier_output_path() {
  case "$1" in
    10k)          echo "$RESULTS_DIR/op-scaling-10k.json" ;;
    50k)          echo "$RESULTS_DIR/op-scaling-50k.json" ;;
    100k)         echo "$RESULTS_DIR/op-scaling-100k.json" ;;
    warm)         echo "$RESULTS_DIR/op-scaling-10k-warm.json" ;;
    100k-warm)    echo "$RESULTS_DIR/op-scaling-100k-warm.json" ;;
    synthetic128) echo "$RESULTS_DIR/op-scaling-10k-synthetic128.json" ;;
  esac
}

tier_harness_cmd() {
  case "$1" in
    10k)
      echo "cargo test --release -F bench --test bench_cold bench_cold_10k_baseline -- --nocapture"
      ;;
    50k)
      echo "cargo test --release -F large_stress --test stress_50k fifty_thousand_docs_v3_cold_probed_validation -- --ignored --nocapture"
      ;;
    100k)
      echo "cargo test --release -F bench --test bench_cold bench_cold_100k_nightly -- --ignored --nocapture"
      ;;
    warm)
      echo "cargo test --release -F bench --test bench_cold bench_cold_10k_warm_vs_cold -- --nocapture"
      ;;
    100k-warm)
      echo "cargo test --release -F bench --test bench_cold bench_cold_100k_warm -- --ignored --nocapture"
      ;;
    synthetic128)
      echo "cargo test --release -F bench --test bench_cold bench_cold_10k_synthetic_128_workload_gate -- --nocapture"
      ;;
  esac
}

run_op_scaling_dry_run() {
  local want_all=true
  if [[ "$#" -gt 0 ]]; then
    want_all=false
  fi
  local -a all_tiers=(10k 50k 100k warm 100k-warm synthetic128)
  local -a planned=()
  local t
  for t in "${all_tiers[@]}"; do
    if $want_all; then
      planned+=("$t")
      continue
    fi
    local arg
    for arg in "$@"; do
      if [[ "$arg" == "$t" ]]; then
        planned+=("$t")
        break
      fi
    done
  done
  if [[ "${#planned[@]}" -eq 0 ]]; then
    echo "run-op-scaling dry-run: no tiers matched args: $*" >&2
    echo "usage: $0 [--dry-run] [10k] [50k] [100k] [warm] [100k-warm] [synthetic128]" >&2
    exit 1
  fi

  echo "run-op-scaling dry-run OK (no cargo/docker; committed JSON unchanged)"
  echo "  OPENPUFFER_ANN_VERSION=3  results_dir=${RESULTS_DIR}"
  echo "  prerequisites: Docker (MinIO testcontainers), Rust release build"
  echo "  full sweep est: ~1–3 h (all tiers); subset = sum of tier estimates below"
  echo ""
  echo "Planned tiers (${#planned[@]}):"
  local tier out note cmd
  for tier in "${planned[@]}"; do
    out="$(tier_output_path "$tier")"
    note="$(tier_est_wall_note "$tier")"
    cmd="$(tier_harness_cmd "$tier")"
    echo "  - ${tier}: ${note}"
    echo "      output: ${out}"
    echo "      harness: OPENPUFFER_ANN_VERSION=3 ${cmd}"
  done
  echo ""
  echo "Would also: cargo build --release --features integration (skipped in dry-run)"
  echo "Post-run: ./scripts/validate-benchmark-json.sh on each op-scaling-*.json (skipped)"
  exit 0
}

if [[ "$DRY_RUN" == "1" ]]; then
  export OPENPUFFER_ANN_VERSION=3
  RESULTS_DIR="$ROOT/benchmarks/results"
  run_op_scaling_dry_run "$@"
fi

export OPENPUFFER_ANN_VERSION=3
export RUST_BACKTRACE=1

RESULTS_DIR="$ROOT/benchmarks/results"
mkdir -p "$RESULTS_DIR"

GIT_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
TIMESTAMP_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

build_release() {
  echo "run-op-scaling: building release openpuffer + bench tests..."
  cargo build --release --features integration
}

extract_bench_json() {
  local log="$1"
  grep -E '^\{' "$log" | tail -1
}

write_op_scaling_json() {
  local tier_label="$1"
  local path_kind="$2"
  local bench_json="$3"
  local harness="$4"
  local out
  if [[ "$path_kind" == "warm" ]]; then
    if [[ "$tier_label" == "100k" ]]; then
      out="$RESULTS_DIR/op-scaling-100k-warm.json"
    else
      out="$RESULTS_DIR/op-scaling-10k-warm.json"
    fi
  elif [[ "$tier_label" == "10k-synthetic128" ]]; then
    out="$RESULTS_DIR/op-scaling-10k-synthetic128.json"
  else
    out="$RESULTS_DIR/op-scaling-${tier_label}.json"
  fi

  python3 - "$tier_label" "$path_kind" "$bench_json" "$harness" "$out" "$GIT_COMMIT" "$TIMESTAMP_UTC" <<'PY'
import json
import sys

tier, path_kind, bench_raw, harness, out_path, git_commit, ts = sys.argv[1:8]
bench = json.loads(bench_raw)


def percentile_ms(samples: list[int], pct: int) -> int:
    """Nearest-rank percentile (matches tests/bench_cold.rs and validate-benchmark-json.sh)."""
    if not samples:
        return 0
    sorted_lat = sorted(int(x) for x in samples)
    n = len(sorted_lat)
    idx = (n * pct + 99) // 100
    if idx == 0:
        idx = 1
    idx -= 1
    if idx >= n:
        idx = n - 1
    return sorted_lat[idx]


docs = int(bench.get("namespace_docs", 0))
dims = int(bench.get("dimensions", 128))
latencies = bench.get("query_latencies_ms") or []
if latencies:
    if len(latencies) != 7:
        raise SystemExit(f"query_latencies_ms must have 7 samples (got {len(latencies)})")
    p50 = percentile_ms(latencies, 50)
    p90 = percentile_ms(latencies, 90)
    p99 = percentile_ms(latencies, 99)
else:
    p50 = int(bench["p50_query_latency_ms"])
    p90 = int(bench.get("p90_query_latency_ms", p50))
    p99 = int(bench.get("p99_query_latency_ms", p90))
ann = int(bench.get("ann_version", 3))

doc_key = {"10k": 10_000, "50k": 50_000, "100k": 100_000, "10k-synthetic128": 10_000}.get(tier)
if doc_key and docs != doc_key:
    raise SystemExit(f"namespace_docs {docs} != expected {doc_key} for tier {tier}")

artifact = {
    "schema_version": "op_scaling_v1",
    "timestamp_utc": ts,
    "git_commit": git_commit,
    "environment": "minio-testcontainers",
    "path": path_kind,
    "namespace_docs": docs,
    "dimensions": dims,
    "p50_ms": p50,
    "p90_ms": p90,
    "p99_ms": p99,
    "storage_roundtrips": bench.get("storage_roundtrips"),
    "recall_at_10": bench.get("recall_at_10"),
    "cold_query_runs": int(bench.get("cold_query_runs") or bench.get("warm_query_runs") or 7),
    "ann_version": ann,
    "cargo_profile": "release",
    "harness": harness,
    "query_latencies_ms": latencies or None,
    "notes": bench.get("notes", f"Unified scaling run {ts} release+v3"),
}
ingest_raw = bench.get("ingest_elapsed_secs") or bench.get("ingest_wall_secs")
if ingest_raw is not None:
    ingest_wall = float(ingest_raw)
    if ingest_wall > 0:
        artifact["ingest_wall_secs"] = round(ingest_wall, 2)
        artifact["docs_per_sec"] = round(docs / ingest_wall, 2)

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(artifact, f, indent=2)
    f.write("\n")
ingest_note = ""
if "docs_per_sec" in artifact:
    ingest_note = f" ingest={artifact['ingest_wall_secs']}s {artifact['docs_per_sec']} docs/s"
print(f"run-op-scaling: wrote {out_path} p50={p50} p90={p90} p99={p99}{ingest_note}")
PY
}

run_tier_10k_cold() {
  local log
  log="$(mktemp)"
  echo "run-op-scaling: 10k cold..."
  cargo test --release -F bench --test bench_cold bench_cold_10k_baseline -- --nocapture 2>&1 | tee "$log"
  local json
  json="$(extract_bench_json "$log")"
  write_op_scaling_json 10k cold "$json" \
    "OPENPUFFER_ANN_VERSION=3 cargo test --release -F bench --test bench_cold bench_cold_10k_baseline -- --nocapture"
  rm -f "$log"
}

run_tier_10k_synthetic128() {
  local log
  log="$(mktemp)"
  echo "run-op-scaling: 10k synthetic-128 workload gate..."
  cargo test --release -F bench --test bench_cold bench_cold_10k_synthetic_128_workload_gate -- --nocapture 2>&1 | tee "$log"
  local json
  json="$(grep -E '"benchmark":"cold_10k_synthetic128"' "$log" | tail -1)"
  write_op_scaling_json 10k-synthetic128 cold "$json" \
    "OPENPUFFER_ANN_VERSION=3 cargo test --release -F bench --test bench_cold bench_cold_10k_synthetic_128_workload_gate -- --nocapture"
  rm -f "$log"
}

run_tier_10k_warm() {
  local log
  log="$(mktemp)"
  echo "run-op-scaling: 10k warm..."
  cargo test --release -F bench --test bench_cold bench_cold_10k_warm_vs_cold -- --nocapture 2>&1 | tee "$log"
  local json
  json="$(grep -E '"benchmark":"warm_10k"' "$log" | tail -1)"
  write_op_scaling_json 10k warm "$json" \
    "OPENPUFFER_ANN_VERSION=3 cargo test --release -F bench --test bench_cold bench_cold_10k_warm_vs_cold -- --nocapture"
  rm -f "$log"
}

run_tier_50k_cold() {
  local log
  log="$(mktemp)"
  echo "run-op-scaling: 50k cold (v3 probed)..."
  cargo test --release -F large_stress --test stress_50k \
    fifty_thousand_docs_v3_cold_probed_validation -- --ignored --nocapture 2>&1 | tee "$log"
  local json
  json="$(extract_bench_json "$log")"
  write_op_scaling_json 50k cold "$json" \
    "OPENPUFFER_ANN_VERSION=3 cargo test --release -F large_stress --test stress_50k fifty_thousand_docs_v3_cold_probed_validation -- --ignored --nocapture"
  rm -f "$log"
}

run_tier_100k_warm() {
  local log
  log="$(mktemp)"
  echo "run-op-scaling: 100k warm (ingest+index+warm; may take ~3–8 min)..."
  cargo test --release -F bench --test bench_cold bench_cold_100k_warm -- --ignored --nocapture 2>&1 | tee "$log"
  local json
  json="$(grep -E '"benchmark":"warm_100k"' "$log" | tail -1)"
  write_op_scaling_json 100k warm "$json" \
    "OPENPUFFER_ANN_VERSION=3 cargo test --release -F bench --test bench_cold bench_cold_100k_warm -- --ignored --nocapture"
  rm -f "$log"
}

run_tier_100k_cold() {
  local log
  log="$(mktemp)"
  echo "run-op-scaling: 100k cold (ingest+index; may take ~8–15 min)..."
  cargo test --release -F bench --test bench_cold bench_cold_100k_nightly -- --ignored --nocapture 2>&1 | tee "$log"
  local json
  json="$(extract_bench_json "$log")"
  write_op_scaling_json 100k cold "$json" \
    "OPENPUFFER_ANN_VERSION=3 cargo test --release -F bench --test bench_cold bench_cold_100k_nightly -- --ignored --nocapture"
  rm -f "$log"
}

want_all=true
if [[ "$#" -gt 0 ]]; then
  want_all=false
fi

build_release

ran=0
run_requested() {
  local tier="$1"
  shift
  if $want_all; then
    return 0
  fi
  local t
  for t in "$@"; do
    if [[ "$t" == "$tier" ]]; then
      return 0
    fi
  done
  return 1
}

if run_requested 10k "$@"; then
  run_tier_10k_cold
  ((ran++)) || true
fi
if run_requested 50k "$@"; then
  run_tier_50k_cold
  ((ran++)) || true
fi
if run_requested 100k "$@"; then
  run_tier_100k_cold
  ((ran++)) || true
fi
if run_requested warm "$@"; then
  run_tier_10k_warm
  ((ran++)) || true
fi
if run_requested 100k-warm "$@"; then
  run_tier_100k_warm
  ((ran++)) || true
fi
if run_requested synthetic128 "$@"; then
  run_tier_10k_synthetic128
  ((ran++)) || true
fi

if [[ "$ran" -eq 0 ]]; then
  echo "run-op-scaling: no tiers matched args: $*" >&2
  echo "usage: $0 [-h|--help] [10k] [50k] [100k] [warm] [100k-warm] [synthetic128]" >&2
  exit 1
fi

if command -v ./scripts/validate-benchmark-json.sh >/dev/null 2>&1; then
  for f in "$RESULTS_DIR"/op-scaling-*.json; do
    [[ -f "$f" ]] || continue
    ./scripts/validate-benchmark-json.sh "$f"
  done
fi

echo "run-op-scaling: done ($ran tier(s))"