# openpuffer vs turbopuffer — scaling comparison report

**Date:** 2026-06-04 (UTC)  
**Scope:** Document-count scaling **shape** on MinIO vs turbopuffer’s **official** 10M × 1024 cold reference—not a live head-to-head at 10M.  
**Related:** [OPENS_VS_TPUF_SCALING_COMPARISON.md](OPENS_VS_TPUF_SCALING_COMPARISON.md) (iteration log), [COMPARISON.md](../COMPARISON.md) § scaling.

---

## Executive summary

**openpuffer is nowhere near turbopuffer’s published absolute cold latency at 10M**, but its measured MinIO tiers show **near-linear growth in document count** (β ≈ 1), which is a plausible *shape* for cold ANN on object storage—turbopuffer publishes only one official doc-count point, so we cannot confirm the same exponent on their side.

On this harness, a four-point linear fit extrapolates openpuffer cold p50 to **~81.5 s** at **10M × 128** (MinIO, release + ANN v3) vs turbopuffer’s official **874 ms** at **10M × 1024** on managed GCP (~**93×** at matched doc count; **~264×** with a √dim heuristic to 1024-d). Back-solving the same linear model says openpuffer would hit **874 ms** at only **~107k docs** on MinIO—that does **not** imply parity at 10M; environment, dimensionality, and fleet architecture dominate.

**One-sentence answer:** At extrapolated 10M scale openpuffer cold latency is on the order of **tens of seconds** on MinIO vs turbopuffer’s **sub-second** official number—**not** the same absolute ballpark—while doc-count scaling on measured tiers is **approximately linear** (β ≈ 1).

---

## Methodology

| Aspect | turbopuffer (official reference) | openpuffer (this report) |
|--------|----------------------------------|---------------------------|
| **Source** | [turbopuffer.com](https://turbopuffer.com) calculator + [tpuf-benchmark](https://github.com/turbopuffer/tpuf-benchmark) | Committed `op-scaling-*.json` from `scripts/run-op-scaling-benchmark.sh` |
| **Scale** | **10M × 1024** (Cohere Wikipedia embeddings) | **10k / 50k / 100k × 128** (synthetic `bench_sin_v1`) |
| **Environment** | Managed GCP (`c2-standard-30`, `gcp-us-central1`) | MinIO testcontainers (`minio-testcontainers`) |
| **Cold path** | `disable_cache: true` ([`vector-10m-cold.toml`](../../benchmarks/specs/tpuf/vector-10m-cold.toml)) | `serve --cache-dir ""`, strong consistency, 7 sequential cold samples |
| **Warm path** | `warm_cache=true`, 100% cache hit before load | `POST …/warm` + eventual ([`op-scaling-10k-warm.json`](../../benchmarks/results/op-scaling-10k-warm.json)) |
| **Load model** | **8 QPS × 30 min**, 1 namespace | Single-client sequential (not 8 QPS sustained) |
| **Query** | Vector ANN, `top_k=10`, cosine | Same protocol shape (`top_k=10`, ANN v3) |
| **Build** | Managed service | `cargo --release`, `OPENPUFFER_ANN_VERSION=3` |
| **Artifact** | [`tpuf-official-reference.json`](../../benchmarks/results/tpuf-official-reference.json) | [`op-scaling-10k.json`](../../benchmarks/results/op-scaling-10k.json), [`50k`](../../benchmarks/results/op-scaling-50k.json), [`100k`](../../benchmarks/results/op-scaling-100k.json), [`10k-synthetic128`](../../benchmarks/results/op-scaling-10k-synthetic128.json) |
| **Extrapolation** | N/A (single published N) | [`compare_op_scaling_to_tpuf.py`](../../benchmarks/report/compare_op_scaling_to_tpuf.py) via `make bench-compare-tpuf` |

---

## Measured data (cold p50, ms)

| Docs | Dims | Label | p50 | p90 | p99 | Environment |
|------|------|-------|-----|-----|-----|-------------|
| 10,000 | 128 | 10k (inline stress) | **86** | 87 | 87 | minio-testcontainers |
| 10,000 | 128 | 10k-synthetic128 (`queries.json`) | **97** | 108 | 108 | minio-testcontainers |
| 50,000 | 128 | 50k | **400** | 405 | 405 | minio-testcontainers |
| 100,000 | 128 | 100k | **824** | 812 | 812 | minio-testcontainers |

**Scaling read (10k → 100k):** **~9.6×** latency for **10×** docs → power-law **β ≈ 0.95**; per-doc p50 stable at **~0.008 ms/doc**.

**Official turbopuffer reference (not re-measured here):**

| Path | Docs × dims | p50 | p90 | p99 |
|------|-------------|-----|-----|-----|
| Cold | 10M × 1024 | **874** | 1214 | 1686 |
| Warm | 10M × 1024 | **14** | 17 | 27 |

---

## Extrapolation and back-solve

**Fit (4 labels, collapsed @ 10k → 91.5 ms mean):** best model **linear**  
\(L \approx 3.68 + 8.15\times10^{-3}\,N\) with **R² = 0.9993**.

| Scale | openpuffer p50 (extrap / estimate) | vs tpuf cold **874 ms** |
|-------|--------------------------------------|-------------------------|
| 1M × 128 | **8,157 ms** (~8.2 s) | ~9.3× |
| 10M × 128 | **81,532 ms** (~81.5 s) | **~93×** |
| 10M × 1024 (√dim heuristic, ×2.83) | **230,608 ms** (~231 s) | **~264×** |
| 10M × 1024 (linear-d estimate, ×8) | **652,259 ms** (~652 s) | **~746×** |
| turbopuffer official | **874 ms** | 1× |

**When would openpuffer match tpuf 874 ms?** (same MinIO harness, 128-d, cold p50—extrapolation only)

| Model | N @ 874 ms |
|-------|------------|
| linear (best) | **~107k** (106,750) |
| power-law | **~109k** (109,492) |
| log-linear | **~161k** (160,552) |

**Per-doc @ 10M (cold p50 / N):** openpuffer extrap **~8153 µs/doc** vs tpuf official **~87 µs/doc** → need **~93×** lower per-doc cold work (or far fewer docs) to approach tpuf’s published point.

**Similar scaling?** **Shape:** yes, roughly linear in N on measured tiers. **Absolute:** no—extrapolated 10M MinIO cold is **orders of magnitude** above tpuf’s GCP managed number; the linear trend will not hold unchanged to 10M (ingest, index layout, S3 parallelism, cache effects).

---

## Limitations

1. **MinIO vs managed S3/GCP** — loopback object storage; no cross-region WAN, no turbopuffer fleet NVMe warm tier.
2. **128-d synthetic vs 1024-d real embeddings** — different probe cost and index geometry; √dim and linear-d rows are **heuristics**, not measurements.
3. **Self-hosted single `serve` vs managed multi-tenant fleet** — no tpuf query-node cache hierarchy or production SPFresh at 10M.
4. **Load model** — 7 sequential cold samples, not **8 QPS × 30 min**; tail latency and queueing differ.
5. **Doc-count curve for tpuf** — only **one** official cold point at 10M; cannot fit β for turbopuffer from public data.
6. **500k tier skipped** — MinIO L2 ingest + index ≫ 45 min on dev host; not in fit set.
7. **Extrapolation to 10M** — unmeasured; linear fit is illustrative, not a competitiveness claim.

---

## What would be needed for a fair test

| Requirement | Why |
|-------------|-----|
| **AWS S3** (or same region as client) for openpuffer | Match object-storage latency class, not MinIO loopback |
| **`TURBOPUFFER_API_KEY`** (test org) | Run live tpuf at matched tiers via `run-tpuf-large-benchmark.sh` / G4 |
| **EC2 in target region** | Same-region client as S3 and tpuf (`aws-us-east-1` or plan region) |
| **Matched workload** | Same `queries.json` / seed / `top_k` / distance; align doc counts where affordable (100k–1M L1/L2 first) |
| **Matched dimensions** | Prefer 128-d on both sides for L1; 1024-d only if budget allows full embedding ingest |
| **Matched load** | Sustained QPS and duration per tpuf spec, or document divergence |
| **G5 measured report** | `render-report.sh` without `--dry-run`; update [COMPARISON.md](../COMPARISON.md) measured rows |

Until then, treat this report as **MinIO scaling shape + official tpuf reference**, not head-to-head at 10M.

---

## Commands to reproduce

```bash
# Regenerate op-scaling JSON (requires Docker for MinIO testcontainers; slow at 50k/100k)
make bench-op-scaling
# Or per tier:
./scripts/run-op-scaling-benchmark.sh 10k
./scripts/run-op-scaling-benchmark.sh 50k
./scripts/run-op-scaling-benchmark.sh 100k

# Print extrapolation, models, back-solve (offline; uses committed JSON)
make bench-compare-tpuf
# Equivalent:
./scripts/compare-op-scaling-to-tpuf.sh

# CI gate (committed artifacts only)
./scripts/verify-op-scaling-comparison.sh
```

**Key files:** `benchmarks/results/op-scaling-*.json`, `benchmarks/results/tpuf-official-reference.json`, `benchmarks/report/compare_op_scaling_to_tpuf.py`, `scripts/compare-op-scaling-to-tpuf.sh`.

---

## Appendix: `make bench-compare-tpuf` output

Captured on the report author host (committed JSON, no re-ingest):

```
./scripts/compare-op-scaling-to-tpuf.sh
=== openpuffer scaling → turbopuffer 10M reference ===

tpuf official cold p50: 874 ms (10M × 1024, GCP, 8 QPS × 30m)

Measured openpuffer cold p50 (MinIO, release + v3):
    10000 docs × 128-d (10k): 86 ms
    10000 docs × 128-d (10k-synthetic128): 97 ms
    50000 docs × 128-d (50k): 400 ms
   100000 docs × 128-d (100k): 824 ms

Collapsed tiers for regression (mean @ duplicate N): [(10000, 91.5), (50000, 400.0), (100000, 824.0)]

### Model comparison (fit on collapsed tiers)
| Model | Formula | R² | RMSE (ms) |
|-------|---------|-----|-----------|
| linear ← best | L ≈ 3.68 + 0.00815287·N | 0.9993 | 8.0 |
| power_law | L ≈ 0.01462 · N^0.948 | 0.9973 | 15.6 |
| log_linear | L ≈ -2672.17 + 295.85·log(N) | 0.9031 | 93.5 |

### Leave-one-out — 2-point fit → predict 3rd tier (collapsed N)
| Held out | actual | predicted | error % |
|----------|--------|-----------|---------|
| N=10,000 | 92 | 75 | -18.4% |
| N=50,000 | 400 | 425 | +6.3% |
| N=100,000 | 824 | 755 | -8.4% |

### Leave-one-out — 4 labels (fit 3 → predict held-out)
| Held out | actual | predicted | error % |
|----------|--------|-----------|---------|
| 10k @ 10,000 | 86 | 96 | +11.1% |
| 10k-synthetic128 @ 10,000 | 97 | 85 | -12.1% |
| 50k @ 50,000 | 400 | 425 | +6.3% |
| 100k @ 100,000 | 824 | 755 | -8.4% |

Best model: **linear** — L ≈ 3.68 + 0.00815287·N

| Scale | p50 (ms) | Notes |
|-------|----------|-------|
| extrap 1M × 128 | **8157** | linear |
| extrap 10M × 128 | **81532** | linear |
| 10M × 1024 (√dim heuristic) | **230608** | ×2.83 on 10M×128 |
| 10M × 1024 (linear-d **estimate**) | **652259** | ×8 brute/O(d); not measured |

### Side-by-side (cold p50)
| System | Docs × dims | Environment | p50 (ms) |
|--------|-------------|-------------|----------|
| turbopuffer (official) | 10M × 1024 | GCP managed | **874** |
| openpuffer (extrapolated) | 10M × 128 | MinIO (linear) | **81532** (81.5s (81532 ms)) |
| openpuffer (√dim estimate) | 10M × 1024 | MinIO + ×2.83 | **230608** (230.6s (230608 ms)) |
| openpuffer (linear-d estimate) | 10M × 1024 | MinIO + ×8 | **652259** (652.3s (652259 ms)) |

√dim heuristic: L(10M,1024) ≈ L(10M,128) × √(1024/128)
Linear-d estimate: L(10M,1024) ≈ L(10M,128) × (1024/128) for brute/dot-dominated work

### When would openpuffer match tpuf 874 ms?
  power_law: N ≈ 109.5k (109,492 docs) @ 128-d
  linear: N ≈ 106.8k (106,750 docs) @ 128-d
  log_linear: N ≈ 160.6k (160,552 docs) @ 128-d

  Per-doc @ 10M: openpuffer extrap 8153.24 µs/doc vs tpuf 87.40 µs/doc → need ~93× improvement

### Are we in the same ballpark vs tpuf 874 ms?
extrapolated openpuffer is **~264× slower** than tpuf 874 ms — **not** in the same absolute ballpark on this MinIO harness

Raw 10M×128 / tpuf: 93.3×
√dim 10M×1024 / tpuf: 263.9×
Linear-d 10M×1024 / tpuf: 746.3×
```

`EXTRAP_JSON` from the same run is stored in CI logs and emitted by `compare-op-scaling-to-tpuf.sh` for automation; see `benchmarks/report/compare_op_scaling_to_tpuf.py`.