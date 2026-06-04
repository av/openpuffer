# openpuffer vs turbopuffer — scaling comparison

**Status:** Iteration 3 (unified measured). All MinIO tiers re-run with **release + `OPENPUFFER_ANN_VERSION=3`** via [`scripts/run-op-scaling-benchmark.sh`](../../scripts/run-op-scaling-benchmark.sh) (2026-06-04). JSON: [`benchmarks/results/op-scaling-*.json`](../../benchmarks/results/op-scaling-10k.json).

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
| T3 (optional) | 500k × 128 | MinIO / AWS | `scripts/bench-large.sh --tier l2` | `large-aws-l2.json` or MinIO schema run |

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

## 3. openpuffer measured columns (Iteration 3 — unified release + v3)

**Host:** dev machine, MinIO via testcontainers.  
**Harness:** [`scripts/run-op-scaling-benchmark.sh`](../../scripts/run-op-scaling-benchmark.sh)  
**Raw JSON:** [`op-scaling-10k.json`](../../benchmarks/results/op-scaling-10k.json), [`op-scaling-50k.json`](../../benchmarks/results/op-scaling-50k.json), [`op-scaling-100k.json`](../../benchmarks/results/op-scaling-100k.json), [`op-scaling-10k-warm.json`](../../benchmarks/results/op-scaling-10k-warm.json).

### Cold query latency (ms)

| Docs | Dims | Environment | p50 | p90 | p99 | ANN | Profile |
|------|------|-------------|-----|-----|-----|-----|---------|
| 10k | 128 | minio-testcontainers | **86** | 95 | 95 | v3 | release |
| 50k | 128 | minio-testcontainers | **388** | 410 | 410 | v3 | release |
| 100k | 128 | minio-testcontainers | **779** | 900 | 900 | v3 | release |

### Warm query latency (ms)

| Docs | Dims | Environment | p50 | p90 | p99 | Notes |
|------|------|-------------|-----|-----|-----|-------|
| 10k | 128 | minio-testcontainers | **88** | 101 | 101 | release + v3; not faster than cold p50 (86 ms) on localhost MinIO — unlike tpuf warm **14 ms** |

### Scaling curve (cold p50 vs doc count)

| namespace_docs | p50_ms (cold) | p50 ms/doc |
|----------------|---------------|------------|
| 10,000 | 86 | 0.0086 |
| 50,000 | 388 | 0.0078 |
| 100,000 | 779 | 0.0078 |

**Doc-count read:** 10k→100k is **~9.1×** latency for **10×** docs (log–log slope **~0.96**). 10k→50k is **~4.5×** for **5×** docs. Overall cold p50 is **near-linear in N** on these three unified points—not sublinear like an ideal fixed-probe curve, but also not worse-than-linear.

**Iteration 2 caveat resolved:** Prior 50k p50 (377 ms) was faster than 10k (717 ms) because of mixed **v2/debug** vs **v3/release**; unified re-run shows monotonic growth 86 → 388 → 779 ms.

---

## 4. Extrapolation and “similar scaling?” rubric

Use these **heuristic** normalizers when comparing MinIO tiers to tpuf’s single 10M point. They are hypotheses to test, not proven models.

### 4.1 Document-count scaling (cold)

Assume cold latency has a fixed overhead plus a term that grows with index size:

\[
L_{\text{op}}(N, d) \approx L_0(d) + k(d)\,N^\beta
\]

Measured openpuffer **β ≈ 0.96** (10k→100k). turbopuffer publishes only **one** \(N\); we cannot fit \(\beta\) for tpuf from official data.

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
| 10k | 86 | ~243,000 | ~278× higher |
| 50k | 388 | ~219,000 | ~251× |
| 100k | 779 | ~220,000 | ~252× |

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

**Shape:** On unified **release + v3** MinIO tiers, cold p50 grows **approximately linearly** with document count (86 → 388 → 779 ms for 10k → 50k → 100k). That is **plausibly similar** to a managed service whose cold latency includes roughly linear index/S3 work, but turbopuffer gives **only one** official doc-count point—we **cannot** confirm the same exponent.

**Absolute values:** Not comparable. MinIO + 128-d synthetic + single-client probes vs GCP + 1024-d Cohere + 8 QPS × 30m.

**One-sentence verdict:** **Near-linear cold growth with N on openpuffer is consistent with a simple linear index-scaling story, but tpuf shape-match remains inconclusive from published data alone; do not interpret MinIO ms as beating or losing to tpuf 874 ms.**

| Gap | Impact |
|-----|--------|
| 10M × 1024 not run locally | Extrapolation only; §4.3 table is hypothetical |
| MinIO vs GCP + managed tpuf | Latency absolute values not comparable; shape-only comparison |
| 128-d synthetic vs Cohere 1024-d | Recall and probe plans differ |
| 7 sequential runs vs 8 QPS × 30m | Percentile stability differs |
| No `TURBOPUFFER_API_KEY` | Official tpuf numbers from homepage + vendored TOML |
| WAL ingest (~1 commit/s) | Write path not part of this scaling study |

### Reproduce

```bash
./scripts/run-op-scaling-benchmark.sh          # all tiers (~4–20 min depending on host)
./scripts/validate-benchmark-json.sh benchmarks/results/op-scaling-*.json
./scripts/test_validate-op-scaling-json.sh
```

---

## 6. Iteration checklist

- [x] Unified v3 + release across 10k / 50k / 100k cold + 10k warm
- [x] p50 / p90 / p99 from 7 samples
- [x] `op-scaling-*.json` + schema validation
- [x] α sweep and §4–§5 conclusion
- [ ] Optional: live tpuf run with API key; AWS large-tier L2/L3 points

---

## 7. Related docs

- Large-tier AWS vs tpuf program: [`PLAN_LARGE_DATASET_BENCHMARK.md`](../PLAN_LARGE_DATASET_BENCHMARK.md), [`COMPARISON.md`](../COMPARISON.md)
- MinIO gates: [`BENCHMARKS.md`](../BENCHMARKS.md)
- Phase 7 exemplar reports: [`BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md`](BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md)