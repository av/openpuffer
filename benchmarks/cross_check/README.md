# Cross-engine id overlap spot-check (Phase 3.3)

Compares **top_k id overlap** between openpuffer and turbopuffer for the first 10 pure vector ANN queries in `queries.json` (`spot_check` block). Uses the same query vectors and `cosine_distance` ANN; different ANN graphs may reduce overlap below 10/10.

## Commands

```bash
# Validate workload (CI-safe, no network)
python3 benchmarks/cross_check/run_spotcheck.py --tier l1 --dry-run

# Offline mock (production-shaped JSON) → benchmarks/results/id-overlap-l1.json
python3 benchmarks/cross_check/run_spotcheck.py --tier l1 --mock

# Live (indexed namespaces on both sides)
export TURBOPUFFER_API_KEY='tpuf_...'
export OPENPUFFER_BASE_URL='http://127.0.0.1:8080'
export OPENPUFFER_BENCH_NAMESPACE='bench-large-100k'
export TURBOPUFFER_BENCH_NAMESPACE='bench-tpuf-2026-06-04-l1'
python3 benchmarks/cross_check/run_spotcheck.py --tier l1
```

Wrapper: `./scripts/run-id-overlap-spotcheck.sh --tier l1 [--dry-run|--mock]`

## Tests

```bash
python3 -m pytest benchmarks/cross_check/test_id_overlap_spotcheck.py -q
```

## Report merge

`scripts/render-report.sh` includes mean overlap@k when `benchmarks/results/id-overlap-{tier}.json` exists (or pass `--overlap-json`).