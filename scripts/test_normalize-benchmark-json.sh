#!/usr/bin/env bash
# Gate: committed benchmark JSON matches normalize-benchmark-json.sh canonical form.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

NORM="$ROOT/scripts/normalize-benchmark-json.sh"
[[ -x "$NORM" ]] || chmod +x "$NORM"

# Idempotent subset (fixtures, manifests, queries, most examples).
"$NORM" --check \
  benchmarks/report/fixtures/large-aws-l1.json \
  benchmarks/report/fixtures/tpuf-l1.json \
  benchmarks/report/fixtures/ingest-large-l1.json \
  benchmarks/workloads/synthetic-128/l1-100k/manifest.json \
  benchmarks/workloads/synthetic-128/l1-100k/queries.json \
  benchmarks/workloads/synthetic-128/l2-500k/manifest.json \
  benchmarks/workloads/synthetic-128/l2-500k/queries.json \
  benchmarks/workloads/synthetic-128/l3-1m/manifest.json \
  benchmarks/workloads/synthetic-128/l3-1m/queries.json \
  benchmarks/cross_check/fixtures/overlap-l1-mock.json \
  benchmarks/results/id-overlap-l1.example.json \
  benchmarks/results/large-aws-l2.example.json \
  benchmarks/results/tpuf-l2.example.json

echo "test_normalize-benchmark-json: ok"