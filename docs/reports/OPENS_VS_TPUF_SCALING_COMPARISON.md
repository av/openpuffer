# openpuffer vs turbopuffer — scaling comparison

**Status:** Iteration 1 (foundation). Official turbopuffer numbers are recorded below; openpuffer columns are **TBD** until Iteration 2 runs local MinIO benches and fills `benchmarks/results/op-scaling-*.json`.

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

We cannot reproduce 10M × 1024 on every dev machine. Iteration 2+ runs **MinIO testcontainers** tiers that stress the same *protocol shape* (strong cold ANN, `top_k=10`, 7 runs, empty `--cache-dir`) at smaller scale.

| Tier | Docs × dims | Environment | Harness | Result artifact (TBD) |
|------|-------------|-------------|---------|------------------------|
| T0 | 10k × 128 | MinIO | `cargo test -F bench bench_cold_10k_baseline` | `baseline-10k.json` (exists) |
| T1 | 50k × 128 | MinIO | `cargo test --release -F large_stress --test stress_50k fifty_thousand_docs_v3_cold_probed_validation -- --ignored` | `cold-50k-v3.json` (partial; no p50 yet) |
| T2 | 100k × 128 | MinIO | `cargo test -F bench bench_cold_100k_nightly -- --ignored` | `nightly-100k.json` (exists) |
| T3 (optional) | 500k × 128 | MinIO / AWS | `scripts/bench-large.sh --tier l2` | `large-aws-l2.json` or MinIO schema run |

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

## 3. openpuffer measured columns (TBD — Iteration 2)

### Cold query p50 (ms)

| Docs | Dims | Environment | p50 | p90 | p99 | vs tpuf @ 10M (normalized) |
|------|------|-------------|-----|-----|-----|----------------------------|
| 10k | 128 | minio-testcontainers | _fill_ | — | — | _fill_ |
| 50k | 128 | minio-testcontainers | _fill_ | — | — | _fill_ |
| 100k | 128 | minio-testcontainers | _fill_ | — | — | _fill_ |
| 500k | 128 | _optional_ | _fill_ | — | — | _fill_ |

Committed snapshots (cold p50 only, for Iteration 2 baseline): `baseline-10k.json` → **662 ms**; `nightly-100k.json` → **6820 ms** (re-validate in Iteration 2; numbers drift with hardware).

### Warm query p50 (ms)

| Docs | Dims | Environment | p50 | Notes |
|------|------|-------------|-----|-------|
| 10k | 128 | minio-testcontainers | _fill_ | From `bench_cold_10k_warm_vs_cold` or dedicated JSON writer |

---

## 4. Extrapolation and “similar scaling?” rubric

Use these **heuristic** normalizers when comparing MinIO tiers to tpuf’s single 10M point. They are hypotheses to test in Iteration 2+, not proven models.

### 4.1 Document-count scaling (cold)

Assume cold latency has a fixed overhead plus a term that grows sublinearly with index size (S3 object fan-out + ANN probe):

\[
L_{\text{op}}(N, d) \approx L_0(d) + k(d)\,N^\alpha,\quad \alpha \in (0, 1]
\]

Fit \(\alpha\) and \(k\) from openpuffer points \((N_i, L_i)\) at fixed \(d=128\). Compare to tpuf’s implied exponent if we had multiple official \(N\) points (we only have **one** tpuf doc count today).

**Per-doc linear proxy** (crude):

\[
\hat{L}_{\text{norm,doc}}(N) = L_{\text{tpuf}}(10^7) \times \frac{N}{10^7}
\]

If openpuffer \(L(N)/N\) is **stable** across 10k→100k while tpuf’s single point implies a different slope, scaling shapes differ.

### 4.2 Dimension scaling

Vector byte volume scales \(\propto d\). For distance-heavy CPU work, a common rough bound is:

\[
L(d) \propto \sqrt{d}
\quad\Rightarrow\quad
L_{\text{norm,dim}} = L(N, d) \times \sqrt{\frac{d_{\text{ref}}}{d}}
\]

Map openpuffer **128-d** to tpuf **1024-d** reference:

\[
L_{\text{op, equiv}} = L_{\text{op}}(N, 128) \times \sqrt{\frac{1024}{128}} = L_{\text{op}}(N, 128) \times 2\sqrt{2}
\]

### 4.3 Combined normalization (single scalar for “ballpark”)

\[
L_{\text{norm}} = L_{\text{op}}(N, d) \times \frac{N_{\text{ref}}}{N} \times \sqrt{\frac{d_{\text{ref}}}{d}}
\]

with \(N_{\text{ref}} = 10^7\), \(d_{\text{ref}} = 1024\). Compare \(L_{\text{norm}}\) to \(L_{\text{tpuf,cold}} = 874\) ms (p50). Ratios **≪1** or **≫1** are expected (self-host MinIO vs managed); the question is whether **log–log slope across openpuffer tiers** resembles a sublinear cold curve.

### 4.4 Warm path

Warm tpuf p50 **14 ms** is cache-resident. openpuffer warm should be measured with pinned cache (`POST /warm`); expect ms–tens-of-ms on localhost MinIO, not comparable to cross-region managed tpuf without identical client placement.

---

## 5. Methodology gaps (honest)

| Gap | Impact |
|-----|--------|
| 10M × 1024 not run locally | Extrapolation only; no openpuffer point at tpuf scale |
| MinIO vs GCP + managed tpuf | Latency absolute values not comparable; shape-only comparison |
| 128-d synthetic vs Cohere 1024-d | Recall and probe plans differ |
| 7 sequential runs vs 8 QPS × 30m | openpuffer lacks sustained-load percentile stability |
| No `TURBOPUFFER_API_KEY` | Official tpuf numbers taken from homepage + vendored TOML |
| WAL ingest (~1 commit/s) | Write path not part of this scaling study |

---

## 6. Iteration 2 checklist

- [ ] Re-run / refresh `bench_cold_10k_baseline`, `bench_cold_100k_nightly`, `stress_50k` v3 cold; record p50/p90/p99 where available
- [ ] Add `benchmarks/results/op-scaling-{10k,50k,100k}.json` with unified schema
- [ ] Run `bench_cold_10k_warm_vs_cold` with JSON export for warm p50
- [ ] Fill §3 tables; compute \(L_{\text{norm}}\) and doc-scaling \(\alpha\) fit
- [ ] State conclusion: **similar / faster-than-linear / slower-than-linear / inconclusive**

---

## 7. Related docs

- Large-tier AWS vs tpuf program: [`PLAN_LARGE_DATASET_BENCHMARK.md`](../PLAN_LARGE_DATASET_BENCHMARK.md), [`COMPARISON.md`](../COMPARISON.md)
- MinIO gates: [`BENCHMARKS.md`](../BENCHMARKS.md)
- Phase 7 exemplar reports: [`BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md`](BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md)