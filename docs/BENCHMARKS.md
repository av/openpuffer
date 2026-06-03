# Benchmarks

Measurable baselines and scale gates for [PLAN_SPFRESH_AND_COLD_1M.md](PLAN_SPFRESH_AND_COLD_1M.md). Work is fact-driven: `@spec` facts under `index/ann` and `query/cold` in `.facts` are checked with `facts check --tags cold,ann` (expect failures until Phases A/B land).

## Feature: `bench`

```bash
# Build integration + bench tests (needs Docker for MinIO testcontainers)
cargo build --features bench -q

# 10k cold baseline (CI-friendly; ~2–4 min on a dev machine with --release serve binary)
cargo test -F bench bench_cold_10k_baseline --test bench_cold -- --nocapture

# Regenerate committed baseline artifact
OPENPUFFER_BENCH_WRITE_BASELINE=1 cargo test -F bench bench_cold_10k_baseline --test bench_cold -- --nocapture
```

The baseline test prints one JSON line containing:

| Field | Meaning |
|-------|---------|
| `storage_roundtrips` | `performance.storage_roundtrips` on a strong cold vector query |
| `s3_get_count` | `GET /v1/debug/cache-stats` → `s3_get_count` after the query |
| `p50_query_latency_ms` | p50 over 7 cold queries (cache reset each run) |
| `candidates_ratio` | ANN candidate pool fraction |
| `index_object_count` | S3 keys under `index/` matching `clusters-*` or `centroids-l1-*.bin` |

Committed snapshot: [`benchmarks/results/baseline-10k.json`](../benchmarks/results/baseline-10k.json).

Phase A gate (ignored until implemented):

```bash
cargo test -F bench bench_cold_10k_storage_roundtrips_at_most_four --test bench_cold -- --include-ignored
```

## Tiers

| Tier | Size | Command | Environment |
|------|------|---------|-------------|
| CI baseline | 10k | `cargo test -F bench bench_cold_10k_baseline` | MinIO testcontainers |
| Nightly | 100k | TBD (`#[ignore]` bench or dedicated job) | MinIO |
| Manual | 1M | [`scripts/bench-1m.sh`](../scripts/bench-1m.sh) | AWS S3 |

## 1M manual (AWS)

1. Configure `OPENPUFFER_S3_*` for AWS.
2. Ingest 1M × 128-dim f32 with `upsert_columns` batches (respect ~1 WAL commit/s; see README 50k stress notes, scaled up).
3. Wait for `index_cursor == wal_commit_seq`.
4. Run `scripts/bench-1m.sh` or cold-query with `--cache-dir=""`.
5. Record `benchmarks/results/1m-aws.json`: `storage_roundtrips ≤ 4`, `recall@10 ≥ 0.85`, p50 **< 600ms**.

## Related tests

- `cargo test -F perf` — 5k in-memory `candidates_ratio < 0.12`
- `cargo test -F integration ten_thousand_docs_indexed_query` — 10k indexed ANN smoke (warm path)
- `cargo test -F integration s3_cold_query_reports_roundtrips_on_minio` — small-namespace cold roundtrips

## Facts

```bash
facts check --tags cold,ann   # @spec gates; fail until Phases A/B
facts ll --tags spec          # list program spec facts
```