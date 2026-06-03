#!/usr/bin/env bash
# Phase 2.3 / G2 MinIO preflight (correctness only — not latency SLOs).
# See docs/PLAN_LARGE_DATASET_BENCHMARK.md §2.3 and docs/BENCHMARKS.md § G2 gates.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> synthetic-128 fixture gates (no Docker)"
cargo test --test synthetic_workload_gate -- --nocapture

echo "==> integration (MinIO testcontainers)"
cargo test -F integration --test integration_s3 synthetic_128_g2_correctness_gates_on_minio -- --nocapture

echo "==> bench cold (10k gates + synthetic-128 workload gate)"
cargo test -F bench --test bench_cold -- --nocapture

echo "G2 MinIO preflight: OK (subset). For full integration suite:"
echo "  cargo test -F integration --test integration_s3 -- --nocapture"
echo "For 100k nightly: cargo test --release -F bench --test bench_cold -- --ignored --nocapture"