# turbopuffer comparison driver (A4)

Python harness that loads the **same synthetic-128 workload** as `scripts/ingest-large.sh` /
`scripts/bench-large.sh` on managed turbopuffer and writes comparison JSON for
`scripts/render-report.sh` (A5).

## Prerequisites

- `TURBOPUFFER_API_KEY` — dedicated test org per [Testing](https://turbopuffer.com/docs/testing)
- `TURBOPUFFER_REGION` — same region as the openpuffer AWS bench host (default `aws-us-east-1`)
- Python 3.9+ and `pip install -r benchmarks/tpuf_driver/requirements.txt`

## Usage

```bash
# Validate config without API calls
python3 benchmarks/tpuf_driver/run_benchmark.py --dry-run
python3 benchmarks/tpuf_driver/run_benchmark.py --dry-run --tier l3

# Full run (creates bench-tpuf-YYYY-MM-DD-{tier}, ingests, benches, deletes namespace)
export TURBOPUFFER_API_KEY='tpuf_...'
export TURBOPUFFER_REGION='aws-us-east-1'
python3 benchmarks/tpuf_driver/run_benchmark.py --tier l1
```

Output: `benchmarks/results/tpuf-l1.json` (schema aligned with `large-aws-l1.json`).

### Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `TURBOPUFFER_API_KEY` | — | Required for live runs |
| `TURBOPUFFER_REGION` | `aws-us-east-1` | API region |
| `TURBOPUFFER_BENCH_TIER` | `l1` | `l1` / `l2` / `l3` |
| `TURBOPUFFER_BENCH_WORKLOAD_DIR` | tier workload path | Override manifest/queries |
| `TURBOPUFFER_BENCH_NAMESPACE` | `bench-tpuf-{date}-{tier}` | Ephemeral namespace |
| `TURBOPUFFER_BENCH_RESULTS` | `benchmarks/results/tpuf-{tier}.json` | Artifact path |
| `TURBOPUFFER_BENCH_SKIP_INGEST` | — | Query-only against existing namespace |
| `TURBOPUFFER_BENCH_SKIP_DELETE` | — | Keep namespace after run (debug) |
| `TURBOPUFFER_BENCH_ENFORCE_GATES` | `1` | Fail if recall@10 &lt; 0.85 or not indexed |

## Protocol

1. **Ingest** — 10k-row `namespace.write(upsert_columns=…)` batches from `generate_synthetic.py`
2. **Index gate** — poll `metadata()` until `index.status == up-to-date` and row count ≥ tier docs
3. **Cold queries** — 7× vector ANN (`consistency: strong`), record `client_total_ms` when present
4. **Recall** — `namespace.recall(num=20, top_k=10)` (same defaults as workload `queries.json`)
5. **Cleanup** — `namespace.delete_all()` unless `TURBOPUFFER_BENCH_SKIP_DELETE=1`

Openpuffer-specific fields (`storage_roundtrips`, `s3_get_count`, `preferred_ann_version`) are
`null` in the JSON so A5 can merge rows without special cases.

## Tests

```bash
pytest benchmarks/tpuf_driver/test_run_benchmark.py -q
```