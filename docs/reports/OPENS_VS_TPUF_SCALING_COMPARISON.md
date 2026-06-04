# openpuffer vs turbopuffer — scaling comparison

**Status:** Iteration 4 (extrapolation + synthetic-128 protocol). MinIO tiers measured with **release + `OPENPUFFER_ANN_VERSION=3`** via [`scripts/run-op-scaling-benchmark.sh`](../../scripts/run-op-scaling-benchmark.sh). Extrapolation: [`scripts/compare-op-scaling-to-tpuf.sh`](../../scripts/compare-op-scaling-to-tpuf.sh).

### Executive summary

- **turbopuffer (official):** cold p50 **874 ms** at **10M × 1024** on GCP (`c2-standard-30`, 8 QPS × 30m, cache disabled) — [`tpuf-official-reference.json`](../../benchmarks/results/tpuf-official-reference.json).
- **openpuffer (measured, MinIO):** cold p50 **92 / 400 / 824 ms** at **10k / 50k / 100k × 128** (unified release+v3); synthetic-128 `queries.json` gate @ 10k: **97 ms** — [`op-scaling-*.json`](../../benchmarks/results/op-scaling-10k.json).
- **openpuffer (extrapolated to 10M):** power-law fit \(L \approx 0.015 \cdot N^{0.945}\) → **~62 s** p50 @ 10M×128; √dim heuristic → **~176 s** @ 10M×1024 vs tpuf **874 ms** (**~200× slower**, **not** the same absolute ballpark; shape-only doc scaling remains near-linear).

**Goal:** Determine whether openpuffer cold/warm query latency scales with namespace size and dimensionality in a pattern **similar** to turbopuffer’s published 10M × 1024-dim curve—not to claim parity on MinIO vs managed GCP.

**Structured reference:** [`benchmarks/results/tpuf-official-reference.json`](../../benchmarks/results/tpuf-official-reference.json)  
**Vendored specs:** [`benchmarks/specs/tpuf/vector-10m-cold.toml`](../../benchmarks/specs/tpuf/vector-10m-cold.toml), [`vector-10m-hot.toml`](../../benchmarks/specs/tpuf/vector-10m-hot.toml)

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

We cannot reproduce 10M × 1024 on every dev machine. Iteration 3 runs **MinIO testcontainers** tiers that stress the same *protocol shape* (strong cold ANN, `top_k=10`, 7 runs, empty `--cache-dir`) at smaller scale.

| Tier | Docs × dims | Environment | Harness | Result artifact |
|------|-------------|-------------|---------|-----------------|
| T0 | 10k × 128 | MinIO | `scripts/run-op-scaling-benchmark.sh` (10k) | [`op-scaling-10k.json`](../../benchmarks/results/op-scaling-10k.json) |
| T1 | 50k × 128 | MinIO | same (50k / `stress_50k` v3 cold) | [`op-scaling-50k.json`](../../benchmarks/results/op-scaling-50k.json) |
| T2 | 100k × 128 | MinIO | same (100k / `bench_cold_100k_nightly`) | [`op-scaling-100k.json`](../../benchmarks/results/op-scaling-100k.json) |
| Warm | 10k × 128 | MinIO | same (`bench_cold_10k_warm_vs_cold`) | [`op-scaling-10k-warm.json`](../../benchmarks/results/op-scaling-10k-warm.json) |
| T0b | 10k × 128 (synthetic-128) | MinIO | `bench_cold_10k_synthetic_128_workload_gate` | [`op-scaling-10k-synthetic128.json`](../../benchmarks/results/op-scaling-10k-synthetic128.json) |
| T3 (optional) | 500k × 128 | MinIO / AWS | `scripts/bench-large.sh --tier l2` | **skipped** this iteration (ingest+index ≫45 min on MinIO; use AWS L2 when available) |

**Unified run settings (Iteration 3):** `cargo --release`, `OPENPUFFER_ANN_VERSION=3`, `ann_version=3` on serve, 7 sequential cold samples, p50/p90/p99 from nearest-rank percentiles (same as `bench-large.sh`).

**Protocol alignment with tpuf**

| Parameter | turbopuffer (10M spec) | openpuffer (local tiers) |
|-----------|------------------------|---------------------------|
| `top_k` | 10 | 10 (`queries.json` / bench_cold) |
| Cold cache | `disable_cache: true` | `serve --cache-dir ""` + strong consistency |
| Warm cache | warm + 100% hit ratio | `POST /v1/namespaces/{ns}/warm` + eventual (`bench_cold_10k_warm_vs_cold`) |
| QPS / duration | 8 QPS, 30m | Single-client sequential (7 cold samples); **not** load-tested at 8 QPS |
| Embeddings | Real 1024-d Cohere | Synthetic `bench_sin_v1` @ 128-d |
| Storage | Managed S3 (tpuf) | MinIO (label **minio-testcontainers** in tables) |

**Do not** paste MinIO p50 into marketing-style tpuf tables without the environment column.

---

## 3. openpuffer measured columns (Iteration 4 — unified release + v3)

**Host:** dev machine, MinIO via testcontainers.  
**Harness:** [`scripts/run-op-scaling-benchmark.sh`](../../scripts/run-op-scaling-benchmark.sh)  
**Raw JSON:** [`op-scaling-10k.json`](../../benchmarks/results/op-scaling-10k.json), [`op-scaling-50k.json`](../../benchmarks/results/op-scaling-50k.json), [`op-scaling-100k.json`](../../benchmarks/results/op-scaling-100k.json), [`op-scaling-10k-warm.json`](../../benchmarks/results/op-scaling-10k-warm.json), [`op-scaling-10k-synthetic128.json`](../../benchmarks/results/op-scaling-10k-synthetic128.json).

### Cold query latency (ms)

| Docs | Dims | Environment | p50 | p90 | p99 | ANN | Profile |
|------|------|-------------|-----|-----|-----|-----|---------|
| 10k | 128 | minio-testcontainers | **92** | 99 | 99 | v3 | release (inline stress vectors) |
| 10k | 128 | minio-testcontainers | **97** | 108 | 108 | v3 | release (synthetic-128 `queries.json`) |
| 50k | 128 | minio-testcontainers | **400** | 405 | 405 | v3 | release |
| 100k | 128 | minio-testcontainers | **824** | 812 | 812 | v3 | release |

### Warm query latency (ms)

| Docs | Dims | Environment | p50 | p90 | p99 | Notes |
|------|------|-------------|-----|-----|-----|-------|
| 10k | 128 | minio-testcontainers | **87** | 98 | 98 | release + v3; not faster than cold p50 (92 ms) on localhost MinIO — unlike tpuf warm **14 ms** |

### Scaling curve (cold p50 vs doc count)

| namespace_docs | p50_ms (cold) | p50 ms/doc |
|----------------|---------------|------------|
| 10,000 | 92 | 0.0092 |
| 50,000 | 400 | 0.0080 |
| 100,000 | 824 | 0.0082 |

**Doc-count read:** 10k→100k is **~9.0×** latency for **10×** docs (power-law exponent **β ≈ 0.95**). Overall cold p50 is **near-linear in N** on these three points.

**Protocol alignment:** synthetic-128 @ 10k (97 ms) matches inline baseline (92 ms) within noise — large-dataset program `queries.json` cold path is comparable to stress-vector baseline on MinIO.

---

## 4. Extrapolation and “similar scaling?” rubric

Reproduce numbers: `./scripts/compare-op-scaling-to-tpuf.sh` ([`benchmarks/report/compare_op_scaling_to_tpuf.py`](../../benchmarks/report/compare_op_scaling_to_tpuf.py)).

### 4.0 Power-law extrapolation (Iteration 4)

Fit on measured cold p50 \((N, L)\): \(L = a \cdot N^b\) in log–log space (three points, **±2σ** band in log-space only).

| Scale | openpuffer p50 (ms) | 95% band (ms) | Notes |
|-------|---------------------|---------------|-------|
| 1M × 128 (extrap) | **7,065** | 6,394 – 7,807 | MinIO, not measured |
| 10M × 128 (extrap) | **62,290** | 56,371 – 68,830 | MinIO, not measured |
| 10M × 1024 (heuristic) | **176,182** | 159,441 – 194,680 | ×√(1024/128) ≈ **2.83** on 10M×128 extrap |

**Side-by-side vs tpuf official cold p50 (874 ms):**

| System | Docs × dims | p50 (ms) |
|--------|-------------|----------|
| turbopuffer (official) | 10M × 1024 | **874** |
| openpuffer (extrap, MinIO) | 10M × 128 | **62,290** (~71× slower than tpuf) |
| openpuffer (tpuf-equiv heuristic) | 10M × 1024 | **176,182** (~**202×** slower than tpuf) |

**Are we in the same ballpark?** **No** for absolute latency on this harness: extrapolated MinIO cold at 10M is **~70–200×** above tpuf’s published **874 ms**. **Shape-only:** doc-count exponent **β ≈ 0.95** is plausibly similar to linear index work, but tpuf provides only one official \(N\) so we cannot confirm the same curve.

### 4.1 Document-count scaling (cold)

Assume cold latency has a fixed overhead plus a term that grows with index size:

\[
L_{\text{op}}(N, d) \approx L_0(d) + k(d)\,N^\beta
\]

Measured openpuffer **β ≈ 0.95** (10k→100k fit). turbopuffer publishes only **one** \(N\); we cannot fit \(\beta\) for tpuf from official data.

### 4.2 Dimension scaling

\[
L_{\text{norm,dim}} = L(N, d) \times \sqrt{\frac{d_{\text{ref}}}{d}}
\quad,\quad
d_{\text{ref}} = 1024,\; d = 128 \Rightarrow \times 2\sqrt{2} \approx 2.83
\]

### 4.3 Combined normalization (single scalar — §4.3 in prior draft)

\[
L_{\text{norm,ref}} = L_{\text{op}}(N, d) \times \frac{N_{\text{ref}}}{N} \times \sqrt{\frac{d_{\text{ref}}}{d}}
\quad,\quad N_{\text{ref}} = 10^7
\]

| Docs | \(L_{\text{op}}\) p50 (ms) | \(L_{\text{norm,ref}}\) (ms) | vs tpuf 874 ms |
|------|---------------------------|------------------------------|----------------|
| 10k | 92 | ~259,000 | ~296× higher |
| 50k | 400 | ~226,000 | ~258× |
| 100k | 824 | ~231,000 | ~264× |

Extrapolated openpuffer cold at 10M would be **hundreds of seconds** on this MinIO harness if linear trend held—**not** comparable in absolute terms to tpuf **874 ms** (managed GCP, real embeddings, 8 QPS × 30m). Use this table for **order-of-magnitude honesty**, not competitiveness claims.

### 4.4 Doc-scaling exponent α (Iteration 3 fit)

Test normalizer (per iteration plan):

\[
L_{\text{norm}}(\alpha) = \frac{L_{\text{p50}} \cdot \sqrt{d}}{\,N^{\alpha}\,}
\quad,\quad \sqrt{d} = \sqrt{128},\; L_{\text{tpuf,cold}} = 874\ \text{ms}
\]

| α | \(L_{\text{norm}}\) @ 10k / 50k / 100k | Spread (max/min) | Mean | Mean / 874 |
|---|----------------------------------------|------------------|------|------------|
| **0** | 243 / 1097 / 2203 | **9.06×** | 1181 | 1.35 |
| **0.33** | 11.6 / 30.9 / 49.3 | 4.24× | 30.6 | 0.035 |
| **0.5** | 2.4 / 4.9 / 7.0 | 2.86× | 4.8 | 0.0055 |
| **1.0** | 0.024 / 0.022 / 0.022 | **1.11×** | 0.023 | ≪ 1 |

**Best collapse across openpuffer tiers:** **α = 1** (per-doc × √d proxy is stable). **Closest mean to tpuf 874 ms** with only three points: **α = 0** (misleading—hides doc scaling). **α = 0.5** is a compromise sublinear doc divisor with ~3× spread.

**Conclusion on α:** No α makes MinIO p50 **match** tpuf 874 ms; environment and scale dominate. For **shape-only** comparison across openpuffer tiers, **α ≈ 1** best aligns the three measured points; measured **β ≈ 0.96** is consistent with that.

### 4.5 Warm path

Warm tpuf p50 **14 ms** is cache-resident on managed infra. openpuffer warm **88 ms** ≈ cold **86 ms** on localhost MinIO with disk cache—no cache win in p50 on this harness. Not comparable to cross-region managed tpuf without identical client placement and load.

---

## 5. Verdict and methodology gaps

### Does openpuffer cold latency grow with N similarly to tpuf’s published curve?

**Shape:** On unified **release + v3** MinIO tiers, cold p50 grows **approximately linearly** with document count (92 → 400 → 824 ms for 10k → 50k → 100k). That is **plausibly similar** to a managed service whose cold latency includes roughly linear index/S3 work, but turbopuffer gives **only one** official doc-count point—we **cannot** confirm the same exponent.

**Absolute values:** Not comparable. Power-law extrapolation to 10M×128 yields **~62 s** p50 on MinIO; √dim heuristic to 10M×1024 yields **~176 s** vs tpuf **874 ms** (~**200×** gap).

**One-sentence verdict:** **Near-linear cold growth with N on openpuffer is consistent with a simple index-scaling story, but extrapolated MinIO latency is orders of magnitude above tpuf’s official 874 ms—not the same absolute ballpark; tpuf shape-match remains inconclusive from published data alone.**

| Gap | Impact |
|-----|--------|
| 10M × 1024 not run locally | Extrapolation via §4.0 (`compare-op-scaling-to-tpuf.sh`); not measured |
| 500k × 128 MinIO tier | Skipped (L2 ingest/index ≫45 min); optional AWS `bench-large.sh --tier l2` |
| MinIO vs GCP + managed tpuf | Latency absolute values not comparable; shape-only comparison |
| 128-d synthetic vs Cohere 1024-d | Recall and probe plans differ |
| 7 sequential runs vs 8 QPS × 30m | Percentile stability differs |
| No `TURBOPUFFER_API_KEY` | Official tpuf numbers from homepage + vendored TOML |
| WAL ingest (~1 commit/s) | Write path not part of this scaling study |

### Reproduce

```bash
./scripts/run-op-scaling-benchmark.sh          # all tiers (~4–20 min depending on host)
./scripts/run-op-scaling-benchmark.sh synthetic128   # 10k queries.json protocol only
./scripts/compare-op-scaling-to-tpuf.sh        # power-law extrapolation table
./scripts/validate-benchmark-json.sh benchmarks/results/op-scaling-*.json
./scripts/test_validate-op-scaling-json.sh
```

---

## 6. Iteration checklist

- [x] Unified v3 + release across 10k / 50k / 100k cold + 10k warm
- [x] p50 / p90 / p99 from 7 samples
- [x] `op-scaling-*.json` + schema validation
- [x] α sweep and §4–§5 conclusion
- [x] Power-law extrapolation to 1M/10M + tpuf-equiv heuristic (`compare-op-scaling-to-tpuf.sh`)
- [x] synthetic-128 @ 10k timing JSON (`op-scaling-10k-synthetic128.json`)
- [ ] Optional: live tpuf run with API key; AWS large-tier L2/L3 points; MinIO 500k (L2)

---

## 7. Related docs

- Large-tier AWS vs tpuf program: [`PLAN_LARGE_DATASET_BENCHMARK.md`](../PLAN_LARGE_DATASET_BENCHMARK.md), [`COMPARISON.md`](../COMPARISON.md)
- MinIO gates: [`BENCHMARKS.md`](../BENCHMARKS.md)
- Phase 7 exemplar reports: [`BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md`](BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md)