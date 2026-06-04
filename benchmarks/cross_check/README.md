# Cross-engine id overlap spot-check (Phase 3.3)

Compares **top_k id overlap** between openpuffer and turbopuffer for the first 10 pure vector ANN queries in `queries.json` (`spot_check` block). Uses the same query vectors and `cosine_distance` ANN; different ANN graphs may reduce overlap below 10/10.

## Commands

```bash
# Validate workload (CI-safe, no network)
python3 benchmarks/cross_check/run_spotcheck.py --tier l1 --dry-run

# Offline mock (production-shaped JSON) → benchmarks/results/id-overlap-l1.json
python3 benchmarks/cross_check/run_spotcheck.py --tier l1 --mock

# Preflight (both namespaces indexed; no queries)
./scripts/preflight-id-overlap.sh --tier l1

# Live (indexed namespaces on both sides; wrapper runs preflight first)
export TURBOPUFFER_API_KEY='tpuf_...'
export OPENPUFFER_BASE_URL='http://127.0.0.1:8080'
export OPENPUFFER_BENCH_NAMESPACE='bench-large-l1'
export TURBOPUFFER_BENCH_NAMESPACE='bench-tpuf-2026-06-04-l1'
./scripts/run-id-overlap-spotcheck.sh --tier l1
```

Wrapper: `./scripts/run-id-overlap-spotcheck.sh --tier l1 [--dry-run|--mock|--preflight-only]`

Empty or missing namespaces exit with actionable errors (e.g. `wal_commit_seq=0`, `approx_row_count=0`) instead of silent zero overlap.

## Tests

```bash
./scripts/install-benchmark-python-deps.sh   # once per host
python3 -m pytest benchmarks/cross_check/test_id_overlap_spotcheck.py -q
```

## Report merge

`scripts/render-report.sh` includes mean overlap@k when `benchmarks/results/id-overlap-{tier}.json` exists (or pass `--overlap-json`).