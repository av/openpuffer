# Synthetic workload embedding functions

The large-dataset comparison program ([`docs/PLAN_LARGE_DATASET_BENCHMARK.md`](../../docs/PLAN_LARGE_DATASET_BENCHMARK.md) G1) stores document vectors **deterministically** from a named `embedding_fn` in each tier’s `manifest.json` / `queries.json`. Both openpuffer and turbopuffer ingest the same floats; benchmark JSON artifacts record `embedding_fn` and `seed` so G5 reports can reject mismatched runs.

**Canonical implementation:** [`generate_synthetic.py`](generate_synthetic.py) (`bench_sin_embedding`, `prng_embedding`, `embedding_for_doc`).

---

## `bench_sin_v1` (default)

**Purpose:** One formula shared across Python generator, Rust G2 gates, cold bench, integration stress, and unit tests—so committed `queries.json` vectors need not live in git as raw batch files.

### Formula

For document index `i` (0-based), dimension `d` (0 … `dim−1`):

```
v[i, d] = sin((i × dim + d) × 0.001)
```

- **Independent of manifest `seed`.** Only `doc_index` and `dim` matter.
- Values are IEEE-754 `f32`/`f64` sines in roughly `[-1, 1]` (not L2-normalized; cosine distance still applies via schema `distance_metric: cosine_distance`).
- **Query vectors** in `queries.json` use the same function at each spec’s `doc_index` (vector, filter, and hybrid queries all embed the reference doc’s vector).

### Parity with Rust tests

| Location | Symbol | Notes |
|----------|--------|--------|
| [`generate_synthetic.py`](generate_synthetic.py) | `bench_sin_embedding()` | Source of truth for ingest batches and `queries.json` |
| [`tests/common/synthetic_workload.rs`](../../tests/common/synthetic_workload.rs) | `bench_sin_embedding()` | G2 upserts + `assert_queries_vectors_match_bench_sin` |
| [`tests/bench_cold.rs`](../../tests/bench_cold.rs) | `synthetic_embedding()` | `DIM = 128`; same loop as above |
| [`tests/stress_50k.rs`](../../tests/stress_50k.rs) | `synthetic_embedding()` | Legacy stress path |
| [`src/index/vector.rs`](../../src/index/vector.rs) | `bench_synthetic_embedding()` (tests) | Recall / routing unit tests |

**Float tolerance:** Python `float` vs Rust `f64` sin agree within **1e−9** on L1 fixtures (`synthetic_workload_gate` uses `epsilon = 1e-9`). Pytest locks the formula at sample indices in [`test_generate_synthetic.py`](test_generate_synthetic.py) (`test_bench_sin_matches_rust_formula`).

**Committed tiers:** All in-repo `synthetic-128` manifests use `"embedding_fn": "bench_sin_v1"` and `"seed": 42`. The seed labels the workload run for reports; it does **not** change `bench_sin_v1` vectors.

### When to use

| Scenario | Use `bench_sin_v1`? |
|----------|---------------------|
| openpuffer ↔ turbopuffer comparison (G3/G4/G5) | **Yes** — required for apples-to-apples |
| MinIO G2 correctness (`synthetic_workload_gate`, `integration_s3`, `bench_cold_10k_*`) | **Yes** — gates assume committed `queries.json` |
| CI / `verify-large-benchmark-program.sh` | **Yes** |
| Nightly or operator runs that must match published `COMPARISON.md` rows | **Yes** |

Regenerate manifests only with the default embedding unless you intentionally fork the program:

```bash
python3 benchmarks/workloads/generate_synthetic.py \
  --output-dir benchmarks/workloads/synthetic-128/l1-100k
# embedding_fn defaults to bench_sin_v1
```

Harnesses propagate `manifest.embedding_fn` into `ingest-large-*.json`, `large-aws-*.json`, and `tpuf-*.json` (`render-report.sh` matches on `embedding_fn` with `seed`, `dimensions`, `namespace_docs`).

---

## `xorshift_f32` (alternate)

**Purpose:** A **seed-dependent** pseudo-random corpus: each doc’s vector comes from a xorshift64 stream keyed by `seed ⊕ doc_index`, producing values in **`[0, 1)`** (via upper 32 bits / 2³²). Useful for experiments that want less geometric structure than sinusoids; **not** used by committed comparison tiers.

### Formula

Initial state: `(seed ^ (doc_index × 0x9E3779B97F4A7C15)) mod 2⁶⁴` (same golden-ratio constant as the generator).

For each dimension: advance state with xorshift64 (`^=`, shifts as in `generate_synthetic.py`), then `component = (state >> 32) / 2³²`.

- **`seed` matters:** same `doc_index`, different `seed` → different vector.
- **Deterministic per (seed, doc_index, dim):** reruns and cross-language ingest must use the same `seed` in `manifest.json` and benchmark JSON.

### Parity with Rust tests

There is **no** Rust helper or gate for `xorshift_f32` today. Parity is enforced only in Python:

- [`test_deterministic_prng_embedding`](test_generate_synthetic.py) — stability and doc-to-doc variation
- Regenerate `manifest.json` / `queries.json` after choosing this mode; **do not** run `cargo test --test synthetic_workload_gate` on those files without updating Rust or switching back to `bench_sin_v1`.

### When to use

| Scenario | Use `xorshift_f32`? |
|----------|---------------------|
| Cross-system latency/recall with **both** sides regenerated from the same manifest | **Yes**, if you document `embedding_fn` + `seed` in every artifact and report |
| Committed `synthetic-128/l1-100k` + G2 MinIO gates | **No** |
| Matching historical `bench_cold` / `integration_s3` stress vectors | **No** — use `bench_sin_v1` |

```bash
python3 benchmarks/workloads/generate_synthetic.py \
  --embedding-fn xorshift_f32 \
  --seed 42 \
  --output-dir /tmp/synthetic-xor-100k
```

**Comparison rule:** openpuffer and turbopuffer must use the **same** `embedding_fn` and `seed` from one manifest. Mixing `bench_sin_v1` on one side and `xorshift_f32` on the other invalidates recall, id-overlap, and G5 row pairing.

---

## Side-by-side summary

| | `bench_sin_v1` | `xorshift_f32` |
|--|----------------|----------------|
| **Depends on `seed`** | No | Yes |
| **Value range** | ≈ `[-1, 1]` (sin) | `[0, 1)` |
| **Rust G2 / fixture tests** | Full parity | Not wired |
| **Committed L1–L3 tiers** | Yes | No |
| **Benchmark JSON field** | `"embedding_fn": "bench_sin_v1"` | `"embedding_fn": "xorshift_f32"` |

---

## Verification commands

```bash
# Python unit tests (formula + PRNG)
pytest benchmarks/workloads/test_generate_synthetic.py -q

# Rust: committed L1 queries.json vectors vs bench_sin_v1
cargo test --test synthetic_workload_gate

# Manifest field present on a tier
jq '{embedding_fn, seed, dim, num_docs}' \
  benchmarks/workloads/synthetic-128/l1-100k/manifest.json
```

---

## Related docs

- Workload layout and tiers: [`synthetic-128/README.md`](synthetic-128/README.md)
- G2 gates: [`docs/BENCHMARKS.md`](../../docs/BENCHMARKS.md) (fixture vectors row)
- Comparison defaults: [`docs/COMPARISON.md`](../../docs/COMPARISON.md)