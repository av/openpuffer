#!/usr/bin/env bash
# Unified openpuffer MinIO scaling benchmarks (release + OPENPUFFER_ANN_VERSION=3).
#
# Runs 10k / 50k / 100k cold and 10k / 100k warm; writes benchmarks/results/op-scaling-*.json.
#
# Usage:
#   ./scripts/run-op-scaling-benchmark.sh           # all tiers
#   ./scripts/run-op-scaling-benchmark.sh 10k warm       # subset
#   ./scripts/run-op-scaling-benchmark.sh 100k-warm      # warm @ 100k (ingest+index ~3–8 min)
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export OPENPUFFER_ANN_VERSION=3
export RUST_BACKTRACE=1

RESULTS_DIR="$ROOT/benchmarks/results"
mkdir -p "$RESULTS_DIR"

GIT_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
TIMESTAMP_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
CARGO_PROFILE=release

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
  echo "usage: $0 [10k] [50k] [100k] [warm] [100k-warm] [synthetic128]" >&2
  exit 1
fi

if command -v ./scripts/validate-benchmark-json.sh >/dev/null 2>&1; then
  for f in "$RESULTS_DIR"/op-scaling-*.json; do
    [[ -f "$f" ]] || continue
    ./scripts/validate-benchmark-json.sh "$f"
  done
fi

echo "run-op-scaling: done ($ran tier(s))"