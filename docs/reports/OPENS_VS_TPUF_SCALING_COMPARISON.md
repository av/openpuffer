# openpuffer vs turbopuffer — scaling comparison (iteration log)

**Status:** Iteration 15 (reconciled 2026-06-05, commits `7f7c0f5` + canonical extrap model). MinIO tiers measured with **release + `OPENPUFFER_ANN_VERSION=3`** via [`scripts/run-op-scaling-benchmark.sh`](../../scripts/run-op-scaling-benchmark.sh). **Canonical user report:** [`BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md`](BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md).

### Executive summary

- **turbopuffer (official):** cold p50 **874 ms** at **10M × 1024** on GCP (`c2-standard-30`, 8 QPS × 30m, cache disabled) — [`tpuf-official-reference.json`](../../benchmarks/results/tpuf-official-reference.json). **Single published doc-count point** for cold; extrapolation uncertainty dominates any ratio vs 874 ms.
- **openpuffer (measured, MinIO):** cold p50 **96 / 412 / 880 ms** at **10k / 50k / 100k × 128**; synthetic-128 @ 10k: **97 ms** — [`op-scaling-*.json`](../../benchmarks/results/op-scaling-10k.json).
- **openpuffer (extrapolated to 10M, canonical linear):** **~87 s** p50 @ 10M×128 (**~100×** tpuf **874 ms** on doc count alone); √dim heuristic → **~247 s** (**~283×**); linear-d estimate → **~699 s** (**~799×**). **Not** validated on AWS or 1024-d. Back-solve **~100k docs** for 874 ms (linear).
- **Superseded conclusions (do not cite):** log_linear on **111/525/813** tiers → **~2.2 s** @ 10M (~**2.5×** tpuf); older linear on **86/400/824** → **~81 s** (~**93×**); anecdotal **~7 s** @ 100k—not in committed JSON.

**Goal:** Determine whether openpuffer cold/warm query latency scales with namespace size and dimensionality in a pattern **similar** to turbopuffer’s published 10M × 1024-dim curve—not to claim parity on MinIO vs managed GCP.

**Structured reference:** [`benchmarks/results/tpuf-official-reference.json`](../../benchmarks/results/tpuf-official-reference.json)  
**Vendored specs:** [`benchmarks/specs/tpuf/vector-10m-cold.toml`](../../benchmarks/specs/tpuf/vector-10m-cold.toml), [`vector-10m-hot.toml`](../../benchmarks/specs/tpuf/vector-10m-hot.toml)

---

## Confidence

| Claim | Confidence | Why |
|-------|------------|-----|
| Measured cold p50 @ 10k–100k × 128 (MinIO) | **High** | Three doc-count tiers + synthetic-128 @ 10k; release+v3; schema-validated JSON |
| Sub-linear 100k tail vs 10k+50k linear bridge | **Medium** | LOO over-predicts 100k; `index_object_count` grows sub-linearly |
| Extrapolated 10M × 128 (~87 s, ~100× tpuf) | **Low** | Unmeasured; **canonical linear** fixed in `compare_op_scaling_to_tpuf.py` |
| √dim / linear-d @ 10M × 1024 | **Low** | Heuristics only; openpuffer bench is 128-d synthetic |
| turbopuffer scaling **shape** vs N | **Low** | **Only one** official cold point at 10M |
| Absolute parity with tpuf **874 ms** | **None** on this harness | MinIO loopback vs GCP managed; different load model |

---

## 1. Official turbopuffer reference (10M × 1024)

| Field | Value |
|-------|--------|
| Documents | 10,000,000 |
| Dimensions | 1024 (Cohere Wikipedia embeddings) |
| Query | Vector ANN, `top_k=10`, cosine |
| Load | **8 QPS**, **30m** duration, 1 namespace |
| Cold path | `disable_cache: true` ([`vector-10m-cold.toml`](../../benchmarks/specs/tpuf/vector-10m-cold.toml)) |
| Warm path | `warm_cache=true`, `wait_for_cache_hit_ratio=1.0` ([`vector-10m-hot.toml`](../../benchmarks/specs/tpuf/vector-10m-hot.toml)) |
| Client / region | **c2-standard-30**, **GCP us-central1** (`TURBOPUFFER_REGION=gcp-us-central1`) |
| Source | [turbopuffer.com](https://turbopuffer.com) calculator + [tpuf-benchmark](https://github.com/turbopuffer/tpuf-benchmark) |

### Latency table (homepage calculator, ms)

| Path | p50 | p90 | p99 |
|------|-----|-----|-----|
| **Cold** (cache disabled) | **874** | **1214** | **1686** |
| **Warm** (cache hot) | **14** | **17** | **27** |

---

## 2. openpuffer analogous benchmark matrix (local, no API key)

| Tier | Docs × dims | Environment | Harness | Result artifact |
|------|-------------|-------------|---------|-----------------|
| T0 | 10k × 128 | MinIO | `scripts/run-op-scaling-benchmark.sh` (10k) | [`op-scaling-10k.json`](../../benchmarks/results/op-scaling-10k.json) |
| T1 | 50k × 128 | MinIO | same (50k / `stress_50k` v3 cold) | [`op-scaling-50k.json`](../../benchmarks/results/op-scaling-50k.json) |
| T2 | 100k × 128 | MinIO | same (100k / `bench_cold_100k_nightly`) | [`op-scaling-100k.json`](../../benchmarks/results/op-scaling-100k.json) |
| Warm | 10k / 100k × 128 | MinIO | `bench_cold_10k_warm_vs_cold` / `bench_cold_100k_warm` | [`op-scaling-10k-warm.json`](../../benchmarks/results/op-scaling-10k-warm.json), [`op-scaling-100k-warm.json`](../../benchmarks/results/op-scaling-100k-warm.json) |
| T0b | 10k × 128 (synthetic-128) | MinIO | `bench_cold_10k_synthetic_128_workload_gate` | [`op-scaling-10k-synthetic128.json`](../../benchmarks/results/op-scaling-10k-synthetic128.json) |
| T3 (optional) | 500k × 128 | MinIO / AWS | `scripts/bench-large.sh --tier l2` | **skipped** (ingest+index ≫45 min on MinIO) |

**Unified run settings:** `cargo --release`, `OPENPUFFER_ANN_VERSION=3`, 7 sequential cold samples, nearest-rank percentiles.

**Do not** paste MinIO p50 into marketing-style tpuf tables without the environment column.

---

## 3. openpuffer measured columns (Iteration 15 — 96 / 412 / 880)

**Host:** dev machine, MinIO via testcontainers.  
**Harness:** [`scripts/run-op-scaling-benchmark.sh`](../../scripts/run-op-scaling-benchmark.sh)

### Cold query latency (ms)

| Docs | Dims | Environment | p50 | p90 | p99 | ANN | Profile |
|------|------|-------------|-----|-----|-----|-----|---------|
| 10k | 128 | minio-testcontainers | **96** | 99 | 99 | v3 | release (inline stress vectors) |
| 10k | 128 | minio-testcontainers | **97** | 108 | 108 | v3 | release (synthetic-128 `queries.json`) |
| 50k | 128 | minio-testcontainers | **412** | 450 | 450 | v3 | release |
| 100k | 128 | minio-testcontainers | **880** | 900 | 900 | v3 | release (`7f7c0f5`) |

### Warm query latency (ms)

| Docs | Dims | Environment | p50 | p90 | p99 | Notes |
|------|------|-------------|-----|-----|-----|-------|
| 10k / 100k | 128 | minio-testcontainers | **112 / 827** | — | — | warm @ `ff47227`; **~59×** tpuf **14 ms** @ 10M; not fleet NVMe tier |

### Scaling curve (cold p50 vs doc count)

| namespace_docs | p50_ms (cold) | p50 ms/doc |
|----------------|---------------|------------|
| 10,000 | 96 | 0.0096 |
| 50,000 | 412 | 0.0082 |
| 100,000 | 880 | 0.0088 |

**Doc-count read:** 10k→100k is **~9.2×** latency for **10×** docs (power-law **β ≈ 0.95** on collapsed tiers).

### 100k tier stability

`query_latencies_ms` in committed JSON span **813–900** (p50=**880**). Prior three-run stability: **813 / 857 / 906** ms (σ≈47 ms). Jitter is host/MinIO noise, not the superseded ~7 s outlier.

---

## 4. Extrapolation and “similar scaling?” rubric

Reproduce: `./scripts/compare-op-scaling-to-tpuf.sh` ([`compare_op_scaling_to_tpuf.py`](../../benchmarks/report/compare_op_scaling_to_tpuf.py)). `EXTRAP_JSON` emits `canonical_model`, `extrap_p50_10m_128_ms`, `ratio_vs_tpuf` (cold extrap), `ratio_warm_10k_vs_tpuf`, `ratio_warm_100k_vs_tpuf`, `warm_ratios_vs_tpuf`, `confidence` (override: `--model=`).

### 4.0 Extrapolation (canonical **linear**)

**Fit points (4):** 10k **96**, 10k-synthetic128 **97**, 50k **412**, 100k **880** (collapsed @ 10k → **96.5 ms** mean).

| Model | Formula (collapsed tiers) | R² | RMSE (ms) | Role |
|-------|---------------------------|-----|-----------|------|
| **linear (canonical)** | \(L \approx -2.9 + 8.73\times10^{-3} N\) | **0.998** | 15 | **All reports / EXTRAP_JSON** |
| power-law | \(L \approx 0.015 \cdot N^{0.950}\) | 0.994 | 25 | diagnostic |
| log_linear | \(L \approx -2848 + 315\log N\) | 0.890 | 107 | superseded on old tiers |

**Extrapolation (canonical = linear)**

| Scale | openpuffer p50 (ms) | vs tpuf **874 ms** |
|-------|---------------------|-------------------|
| 1M × 128 (extrap) | **~8,729** | ~10× |
| 10M × 128 (extrap) | **~87,321** | **~100×** |
| 10M × 1024 (√dim **estimate**) | **~246,981** | **~283×** |
| 10M × 1024 (linear-d **estimate**) | **~698,567** | **~799×** |

**Are we in the same ballpark?** Extrapolated MinIO cold at 10M is **~100–283×** above tpuf **874 ms**—**not** parity. turbopuffer provides **one** official cold point; treat any 10M ratio as **low confidence**.

**When would openpuffer match tpuf 874 ms?** (back-solve on this harness, 128-d): **~100k** docs (linear).

---

## 5. Verdict

**Shape:** Cold p50 grows roughly with doc count (β ≈ 0.95); near-linear ms/doc across measured tiers.

**Absolute values:** Canonical **linear** extrap **~87 s** @ 10M×128 vs tpuf **874 ms** is **~100× slower**—**not** the superseded log_linear **~2.5×** or prior **~81 s** / **~93×** narratives.

**One-sentence verdict:** **Measured MinIO tiers 96/412/880 ms suggest near-linear cold growth; canonical linear extrap gives ~87 s @ 10M×128 (~100× tpuf’s single 10M GCP point, low confidence)—not validated at 10M, AWS, or 1024-d.**

---

## 6. Reproduce

```bash
./scripts/run-op-scaling-benchmark.sh          # all tiers
./scripts/run-op-scaling-benchmark.sh 100k   # 100k only (~3 min)
./scripts/compare-op-scaling-to-tpuf.sh
./scripts/print-scaling-verdict.sh
make bench-compare-tpuf
make bench-op-scaling
./scripts/test_compare-op-scaling-to-tpuf.sh
./scripts/verify-op-scaling-comparison.sh
```

---

## 7. Iteration checklist

- [x] Unified v3 + release across 10k / 50k / 100k cold + 10k / 100k warm
- [x] Tier refresh @ `7f7c0f5` (96 / 412 / 880 ms)
- [x] Canonical **linear** extrap model (no auto-switch to log_linear)
- [x] `EXTRAP_JSON`: `canonical_model`, `ratio_vs_tpuf` (cold), `ratio_warm_*_vs_tpuf`, `warm_ratios_vs_tpuf`, `confidence`
- [x] Reconcile docs vs superseded log_linear ~2.5× and ~81 s linear stories
- [ ] Optional: live tpuf run; AWS L2/L3; MinIO 500k

---

## 8. Related docs

- User report: [`BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md`](BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md)
- [`COMPARISON.md`](../COMPARISON.md) § scaling
- [`PLAN_LARGE_DATASET_BENCHMARK.md`](../PLAN_LARGE_DATASET_BENCHMARK.md)