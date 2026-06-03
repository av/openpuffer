# synthetic-128 workload

Deterministic **128-dimensional f32** corpus for openpuffer ↔ turbopuffer comparison runs ([`docs/PLAN_LARGE_DATASET_BENCHMARK.md`](../../../docs/PLAN_LARGE_DATASET_BENCHMARK.md) Phase 1).

## Generator

```bash
# Default L1 tier (100k docs) — manifest + queries only (no vectors in git)
python3 benchmarks/workloads/generate_synthetic.py \
  --output-dir benchmarks/workloads/synthetic-128/l1-100k

# Include upsert batch JSON (10k rows each) for scripted ingest
python3 benchmarks/workloads/generate_synthetic.py \
  --output-dir /tmp/synthetic-100k \
  --write-batches

# L2 / L3 tiers
python3 benchmarks/workloads/generate_synthetic.py --num-docs 500000 --output-dir benchmarks/workloads/synthetic-128/l2-500k
python3 benchmarks/workloads/generate_synthetic.py --num-docs 1000000 --output-dir benchmarks/workloads/synthetic-128/l3-1m
```

## Embedding function

| `embedding_fn` | Formula | Use when |
|----------------|---------|----------|
| **`bench_sin_v1`** (default) | `sin((doc_index × dim + d) × 0.001)` | Matching `bench_cold.rs`, `integration_s3` stress, existing recall gates |
| `xorshift_f32` | xorshift64 stream from `seed ⊕ doc_index` | Fresh PRNG corpus; document `seed` in report |

## Artifacts

| File | Purpose |
|------|---------|
| `manifest.json` | `seed`, `num_docs`, `dim`, `batch_size`, id/attribute definitions, ingest cadence |
| `queries.json` | 50 vector + 6 filter + 4 hybrid query specs (precomputed vectors) |
| `batches/batch-*.json` | Optional; `upsert_columns` bodies for curl/SDK ingest |

## Ingest (openpuffer)

Use [`scripts/ingest-large.sh`](../../../scripts/ingest-large.sh) (A2): generates batches from this manifest, POSTs with cadence, polls meta until indexed.

```bash
# L1 (100k) — uses committed l1-100k/manifest.json
export OPENPUFFER_S3_ENDPOINT=... OPENPUFFER_S3_BUCKET=... OPENPUFFER_S3_ACCESS_KEY=... OPENPUFFER_S3_SECRET_KEY=...
./scripts/ingest-large.sh --tier l1

# Dry-run (toolchain + plan only; no S3)
./scripts/ingest-large.sh --dry-run
./scripts/ingest-large.sh --tier l2 --dry-run
./scripts/ingest-large.sh --tier l3 --dry-run

# Re-use pre-generated batch dir
OPENPUFFER_INGEST_BATCH_DIR=/tmp/synthetic-100k ./scripts/ingest-large.sh
```

Manual curl loop (same cadence) if needed:

```bash
BASE=http://127.0.0.1:8080/v2/namespaces/bench-large-100k
for f in /tmp/synthetic-100k/batches/batch-*.json; do
  curl -sf -X POST "$BASE" -H 'Content-Type: application/json' -d @"$f"
  sleep 1.1
done
# Poll: GET /v1/namespaces/bench-large-100k until index_cursor == wal_commit_seq, preferred_ann_version == 3
```

## Tiers (plan)

| Tier | Docs | Committed manifest dir |
|------|------|------------------------|
| L1 | 100k | `l1-100k/` |
| L2 | 500k | `l2-500k/` |
| L3 | 1M | `l3-1m/` |

Manifest + `queries.json` only (no `batches/` in git). Regenerate with `generate_synthetic.py` if schema changes.

**Default comparison tier: L1 (100k).** L2/L3 for stress and 1M cold SLO runs.

```bash
./scripts/bench-large.sh --tier l2 --dry-run
./scripts/bench-large.sh --tier l3 --dry-run
```

## Correctness gates (G2)

Rust tests load this directory’s `manifest.json` / `queries.json` (no batch vectors required in git):

| Test | Command |
|------|---------|
| Fixture consistency | `cargo test --test synthetic_workload_gate` |
| Integration smoke (10k MinIO) | `cargo test -F integration --test integration_s3 synthetic_128_g2_correctness_gates_on_minio` |
| Bench cold gate | `cargo test -F bench --test bench_cold bench_cold_10k_synthetic_128_workload_gate` |

Preflight script: [`scripts/run-minio-correctness-gates.sh`](../../../scripts/run-minio-correctness-gates.sh). See [`docs/BENCHMARKS.md`](../../../docs/BENCHMARKS.md) § G2.