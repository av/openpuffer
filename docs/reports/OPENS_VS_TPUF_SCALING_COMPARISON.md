# openpuffer vs turbopuffer — scaling comparison (iteration log)

**Status:** Iteration 9 (reconciled 2026-06-05, commits `da45441` / `9c637d1`). MinIO tiers measured with **release + `OPENPUFFER_ANN_VERSION=3`** via [`scripts/run-op-scaling-benchmark.sh`](../../scripts/run-op-scaling-benchmark.sh). **Canonical user report:** [`BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md`](BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md).

### Executive summary

- **turbopuffer (official):** cold p50 **874 ms** at **10M × 1024** on GCP (`c2-standard-30`, 8 QPS × 30m, cache disabled) — [`tpuf-official-reference.json`](../../benchmarks/results/tpuf-official-reference.json). **Single published doc-count point** for cold.
- **openpuffer (measured, MinIO):** cold p50 **111 / 525 / 813 ms** at **10k / 50k / 100k × 128** (unified sweep); synthetic-128 @ 10k: **97 ms** — [`op-scaling-*.json`](../../benchmarks/results/op-scaling-10k.json).
- **openpuffer (extrapolated to 10M, 4-point fit):** best model **log_linear** → **~2.2 s** p50 @ 10M×128 (**~2.5×** tpuf **874 ms** on doc count alone); √dim heuristic → **~6.1 s** (**~7×**); linear-d estimate → **~17 s** (**~20×**). **Not** validated on AWS or 1024-d. Back-solve **~99–137k docs** for 874 ms (model-dependent).
- **100k stability (three release runs, same harness):** p50 **813 / 857 / 906 ms** (median **857**, σ≈**47 ms**, ±6% vs median). Committed fit uses sweep value **813 ms**.
- **Superseded conclusions (do not cite):** linear-only fit on older **86/400/824 ms** tiers extrapolated **~81 s** @ 10M (~**93×** slower); anecdotal **~7 s** @ 100k or **~70×** @ 10M from debug build or host contention—not in committed `op-scaling-*.json`.

**Goal:** Determine whether openpuffer cold/warm query latency scales with namespace size and dimensionality in a pattern **similar** to turbopuffer’s published 10M × 1024-dim curve—not to claim parity on MinIO vs managed GCP.

**Structured reference:** [`benchmarks/results/tpuf-official-reference.json`](../../benchmarks/results/tpuf-official-reference.json)  
**Vendored specs:** [`benchmarks/specs/tpuf/vector-10m-cold.toml`](../../benchmarks/specs/tpuf/vector-10m-cold.toml), [`vector-10m-hot.toml`](../../benchmarks/specs/tpuf/vector-10m-hot.toml)

---

## Confidence

| Claim | Confidence | Why |
|-------|------------|-----|
| Measured cold p50 @ 10k–100k × 128 (MinIO) | **High** | Three doc-count tiers + synthetic-128 @ 10k; release+v3; schema-validated JSON |
| Sub-linear 100k tail vs 10k+50k linear bridge | **Medium** | LOO over-predicts 100k by ~30%; `index_object_count` 72→236→269; 100k p50 varies ±6% across reruns |
| Extrapolated 10M × 128 (~2–2.5× tpuf) | **Low** | Unmeasured; model choice (log_linear vs prior linear) swings 10M×128 from **~2 s** to **~80 s** |
| √dim / linear-d @ 10M × 1024 | **Low** | Heuristics only; openpuffer bench is 128-d synthetic |
| turbopuffer scaling **shape** vs N | **Low** | Only one official cold point at 10M |
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
| Warm | 10k × 128 | MinIO | same (`bench_cold_10k_warm_vs_cold`) | [`op-scaling-10k-warm.json`](../../benchmarks/results/op-scaling-10k-warm.json) |
| T0b | 10k × 128 (synthetic-128) | MinIO | `bench_cold_10k_synthetic_128_workload_gate` | [`op-scaling-10k-synthetic128.json`](../../benchmarks/results/op-scaling-10k-synthetic128.json) |
| T3 (optional) | 500k × 128 | MinIO / AWS | `scripts/bench-large.sh --tier l2` | **skipped** (ingest+index ≫45 min on MinIO) |

**Unified run settings:** `cargo --release`, `OPENPUFFER_ANN_VERSION=3`, 7 sequential cold samples, nearest-rank percentiles.

**Do not** paste MinIO p50 into marketing-style tpuf tables without the environment column.

---

## 3. openpuffer measured columns (Iteration 9 — unified release + v3)

**Host:** dev machine, MinIO via testcontainers.  
**Harness:** [`scripts/run-op-scaling-benchmark.sh`](../../scripts/run-op-scaling-benchmark.sh)

### Cold query latency (ms)

| Docs | Dims | Environment | p50 | p90 | p99 | ANN | Profile |
|------|------|-------------|-----|-----|-----|-----|---------|
| 10k | 128 | minio-testcontainers | **111** | 128 | 128 | v3 | release (inline stress vectors) |
| 10k | 128 | minio-testcontainers | **97** | 108 | 108 | v3 | release (synthetic-128 `queries.json`) |
| 50k | 128 | minio-testcontainers | **525** | 595 | 595 | v3 | release |
| 100k | 128 | minio-testcontainers | **813** | 900 | 900 | v3 | release (tier sweep; see stability below) |

### Warm query latency (ms)

| Docs | Dims | Environment | p50 | p90 | p99 | Notes |
|------|------|-------------|-----|-----|-----|-------|
| 10k | 128 | minio-testcontainers | **81** | — | — | release + v3; warm path eliminates cold S3 batch; still not tpuf-class **14 ms** at 10M |

### Scaling curve (cold p50 vs doc count)

| namespace_docs | p50_ms (cold) | p50 ms/doc |
|----------------|---------------|------------|
| 10,000 | 111 | 0.0111 |
| 50,000 | 525 | 0.0105 |
| 100,000 | 813 | 0.0081 |

**Doc-count read:** 10k→100k is **~7.3×** latency for **10×** docs (power-law **β ≈ 0.91**). **100k is sub-linear** vs a linear bridge from 10k+50k (measured 813 ms vs ~1054 ms predicted).

### 100k tier stability (2026-06-05)

Three consecutive `run-op-scaling-benchmark.sh 100k` runs (release, same harness):

| Run | p50 (ms) | Notes |
|-----|----------|-------|
| Tier sweep (`9c637d1`) | **813** | Committed in `op-scaling-100k.json` |
| Stability rerun 1 | **857** | +5.4% vs sweep |
| Stability rerun 2 | **906** | +11.4% vs sweep |

**Variance:** min **813**, max **906**, median **857**, mean **859**, σ≈**47 ms** (~±6% vs median). **Not unstable** at order-of-magnitude level; jitter is host/MinIO/container noise and fresh ingest/index layout (`index_object_count` 269–280), not the superseded ~7 s outlier.

---

## 4. Extrapolation and “similar scaling?” rubric

Reproduce: `./scripts/compare-op-scaling-to-tpuf.sh` ([`compare_op_scaling_to_tpuf.py`](../../benchmarks/report/compare_op_scaling_to_tpuf.py)). `EXTRAP_JSON` includes a `notes[]` field flagging superseded linear-fit (~81 s @ 10M) and 100k stability.

### 4.0 Extrapolation (Iteration 9 — log_linear best)

**Fit points (4):** 10k **111**, 10k-synthetic128 **97**, 50k **525**, 100k **813** (collapsed @ 10k → **104 ms** mean).

| Model | Formula (collapsed tiers) | R² | RMSE (ms) |
|-------|---------------------------|-----|-----------|
| **log_linear (best)** | \(L \approx -2671 + 300\log N\) | **0.986** | 34 |
| linear | \(L \approx 65 + 7.79\times10^{-3} N\) | 0.971 | 50 |
| power-law | \(L \approx 0.024 \cdot N^{0.913}\) | 0.969 | 51 |

**Extrapolation (best = log_linear)**

| Scale | openpuffer p50 (ms) | vs tpuf **874 ms** |
|-------|---------------------|-------------------|
| 1M × 128 (extrap) | **~1,470** | ~1.7× |
| 10M × 128 (extrap) | **~2,160** | **~2.5×** |
| 10M × 1024 (√dim **estimate**) | **~6,111** | **~7×** |
| 10M × 1024 (linear-d **estimate**) | **~17,283** | **~20×** |

**Are we in the same ballpark?** Extrapolated MinIO cold at 10M is **~2–7×** above tpuf **874 ms** depending on dim heuristic—not **~70×** or **~90×** from the superseded linear-only narrative. Still **not** parity: different environment, dims, and load; 10M is **unmeasured**.

**When would openpuffer match tpuf 874 ms?** (back-solve on this harness, 128-d): **~99k–137k** docs (model-dependent).

---

## 5. Verdict

**Shape:** Cold p50 grows roughly with doc count (β ≈ 0.91); 100k shows **sub-linear** tail vs naive linear bridge.

**Absolute values:** Extrapolated **~2.2 s** @ 10M×128 vs tpuf **874 ms** is **same order of magnitude** under heroic log-linear extrap—not the prior **~81 s** / **~93×** story. √dim-adjusted **~6–20×** remains illustrative only.

**One-sentence verdict:** **Measured MinIO tiers suggest log-linear-ish cold growth with plausible ~2–7× gap vs tpuf’s single 10M GCP point on extrapolation—not validated at 10M, AWS, or 1024-d; earlier ~70× / ~7 s @ 100k claims are superseded.**

---

## 6. Reproduce

```bash
./scripts/run-op-scaling-benchmark.sh          # all tiers
./scripts/run-op-scaling-benchmark.sh 100k   # 100k only (~3 min)
./scripts/compare-op-scaling-to-tpuf.sh
make bench-compare-tpuf
make bench-op-scaling
./scripts/test_compare-op-scaling-to-tpuf.sh
./scripts/verify-op-scaling-comparison.sh
```

---

## 7. Iteration checklist

- [x] Unified v3 + release across 10k / 50k / 100k cold + 10k warm
- [x] Tier sweep refresh @ `da45441` / `9c637d1` (111 / 525 / 813 ms)
- [x] 100k stability: three runs, variance documented
- [x] Reconcile docs vs superseded ~81 s / ~93× / ~7 s @ 100k
- [x] `EXTRAP_JSON.notes[]` for outlier history
- [x] Confidence section
- [ ] Optional: live tpuf run; AWS L2/L3; MinIO 500k

---

## 8. Related docs

- User report: [`BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md`](BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md)
- [`COMPARISON.md`](../COMPARISON.md) § scaling
- [`PLAN_LARGE_DATASET_BENCHMARK.md`](../PLAN_LARGE_DATASET_BENCHMARK.md)