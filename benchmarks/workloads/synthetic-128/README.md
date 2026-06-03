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

1. Set `OPENPUFFER_ANN_VERSION=3` on `serve` before first write.
2. POST each batch to `/v2/namespaces/{ns}`; first batch includes `schema` from generator.
3. Sleep **~1.1s** between batches ([`docs/BENCHMARKS.md`](../../../docs/BENCHMARKS.md#1m-ingest-cadence)).
4. Poll `GET /v1/namespaces/{ns}` until `index_cursor == wal_commit_seq` and `preferred_ann_version == 3`.

Example with generated batches:

```bash
BASE=http://127.0.0.1:8080/v2/namespaces/bench-large-100k
for f in /tmp/synthetic-100k/batches/batch-*.json; do
  curl -sf -X POST "$BASE" -H 'Content-Type: application/json' -d @"$f"
  sleep 1.1
done
```

## Tiers (plan)

| Tier | Docs | Committed manifest dir |
|------|------|------------------------|
| L1 | 100k | `l1-100k/` |
| L2 | 250k–500k | generate locally |
| L3 | 1M | generate locally |

**Default comparison tier: L1 (100k).**