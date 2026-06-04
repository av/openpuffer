#!/usr/bin/env bash
# Gate: op-scaling JSON artifacts validate against op-scaling.schema.json.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

for f in benchmarks/results/op-scaling-10k.json \
  benchmarks/results/op-scaling-50k.json \
  benchmarks/results/op-scaling-100k.json \
  benchmarks/results/op-scaling-10k-warm.json \
  benchmarks/results/op-scaling-10k-synthetic128.json; do
  if [[ ! -f "$f" ]]; then
    echo "test_validate-op-scaling-json: skip missing $f" >&2
    continue
  fi
  ./scripts/validate-benchmark-json.sh "$f"
done

echo "test_validate-op-scaling-json: OK"