# turbopuffer comparison driver (A4)

Python harness that loads the **same synthetic-128 workload** as `scripts/ingest-large.sh` /
`scripts/bench-large.sh` on managed turbopuffer and writes comparison JSON for
`scripts/render-report.sh` (A5).

## Prerequisites

- `TURBOPUFFER_API_KEY` — dedicated test org per [Testing](https://turbopuffer.com/docs/testing)
- `TURBOPUFFER_REGION` — same region as the openpuffer AWS bench host (default `aws-us-east-1`; map `us-east-1` → `aws-us-east-1`)
- Python 3.11+ and `./scripts/install-benchmark-python-deps.sh` (or `pip install -r benchmarks/requirements.txt`)
- Preflight: [`scripts/preflight-tpuf.sh`](../../scripts/preflight-tpuf.sh) (region RTT, cost estimate, artifact scan)

## Usage

**Operator wrapper (G4 — recommended):** runs G2 subset, API key/region preflight, then this driver.

```bash
./scripts/run-tpuf-large-benchmark.sh --dry-run
./scripts/preflight-tpuf.sh --tier l1 --skip-rtt   # offline (no API key)
./scripts/run-tpuf-large-benchmark.sh --preflight-only --tier l1

export TURBOPUFFER_API_KEY='tpuf_...'
export TURBOPUFFER_REGION='aws-us-east-1'
export TURBOPUFFER_BENCH_DELETE_FIRST=1
./scripts/preflight-tpuf.sh --tier l1
./scripts/run-tpuf-large-benchmark.sh --tier l1
./scripts/preflight-tpuf.sh --check-results benchmarks/results/tpuf-l1.json
```

**Direct driver** (same protocol, no G2 preflight):

```bash
python3 benchmarks/tpuf_driver/run_benchmark.py --dry-run
python3 benchmarks/tpuf_driver/run_benchmark.py --dry-run --tier l3

export TURBOPUFFER_API_KEY='tpuf_...'
export TURBOPUFFER_REGION='aws-us-east-1'
python3 benchmarks/tpuf_driver/run_benchmark.py --tier l1
./scripts/run-tpuf-large-benchmark.sh --tier l1 --warm
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
| `TURBOPUFFER_BENCH_DELETE_FIRST` | `1` | `delete_all` before ingest (re-runs) |
| `TURBOPUFFER_BENCH_SKIP_DELETE` | — | Keep namespace after run (debug; ongoing storage cost) |
| `TURBOPUFFER_BENCH_ENFORCE_GATES` | `1` | Fail if recall@10 &lt; 0.85 or not indexed |
| `TURBOPUFFER_INGEST_START_BATCH` | `1` | Resume ingest at this **1-based** batch (after prior batches succeeded) |
| `TURBOPUFFER_INGEST_RETRY_MAX` | `6` | Max attempts per upsert batch (transient API errors only) |
| `TURBOPUFFER_INGEST_RETRY_BASE_MS` | `500` | Exponential backoff base (ms) |
| `TURBOPUFFER_INGEST_RETRY_MAX_MS` | `30000` | Backoff cap (ms) |

### Ingest retry / resume (production)

Mirrors [`scripts/ingest-large.sh`](../../scripts/ingest-large.sh): transient **429**, **5xx**, connection resets, and timeouts are retried with exponential backoff. On exhaustion the driver writes **partial** `tpuf-{tier}.json` with `ingest_failures`, `ingest_status: failed`, and `ingest_resume.next_batch`, then exits non-zero.

```bash
# Re-run from batch 6 after batches 1–5 succeeded (same namespace; keep DELETE_FIRST=0 if resuming)
export TURBOPUFFER_BENCH_DELETE_FIRST=0
export TURBOPUFFER_INGEST_START_BATCH=6
python3 benchmarks/tpuf_driver/run_benchmark.py --tier l1
```

Use `TURBOPUFFER_BENCH_DELETE_FIRST=0` when resuming so prior rows are not wiped. Batch size stays **10k** per workload `manifest.json`.

## Protocol

1. **Ingest** — 10k-row `namespace.write(upsert_columns=…)` batches from `generate_synthetic.py` (retry/resume above)
2. **Index gate** — poll `metadata()` until `index.status == up-to-date` and row count ≥ tier docs
3. **Cold queries** — 7× vector ANN (`consistency: strong`), record `client_total_ms` when present
4. **Filter + hybrid** — all `filter_queries` / `hybrid_queries` from `queries.json` (1× each, strong); per-query latency in `filter_query_runs` / `hybrid_query_runs`
5. **Warm (optional `--warm`)** — `hint_cache_warm` then 20× vector ANN (`warm_query_protocol`, `consistency: eventual`); `p50_warm_query_latency_ms` / `p95_warm_query_latency_ms`
6. **Recall** — `namespace.recall(num=20, top_k=10)` (same defaults as workload `queries.json`)
7. **Cleanup** — `delete_all` before ingest when `TURBOPUFFER_BENCH_DELETE_FIRST=1`; `delete_all` in `finally` unless `TURBOPUFFER_BENCH_SKIP_DELETE=1`

Full operator runbook: [docs/BENCHMARKS.md § G4](../../docs/BENCHMARKS.md#g4-turbopuffer-operator-setup).

Openpuffer-specific fields (`storage_roundtrips`, `s3_get_count`, `preferred_ann_version`) are
`null` in the JSON so A5 can merge rows without special cases.

## Tests

```bash
pytest benchmarks/tpuf_driver/test_run_benchmark.py -q
```