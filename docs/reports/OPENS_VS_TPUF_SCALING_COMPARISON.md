# openpuffer vs turbopuffer — scaling comparison

**Status:** Iteration 2 (measured). openpuffer MinIO tiers recorded in `benchmarks/results/op-scaling-*.json` (2026-06-04). Normalized scaling analysis and conclusion remain **TBD** (Iteration 3).

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

| Tier | Docs × dims | Environment | Harness | Result artifact |
|------|-------------|-------------|---------|-----------------|
| T0 | 10k × 128 | MinIO | `cargo test -F bench bench_cold_10k_baseline` | [`op-scaling-10k.json`](../../benchmarks/results/op-scaling-10k.json) |
| T1 | 50k × 128 | MinIO | `cargo test --release -F large_stress --test stress_50k fifty_thousand_docs_v3_cold_probed_validation -- --ignored` | [`op-scaling-50k.json`](../../benchmarks/results/op-scaling-50k.json) |
| T2 | 100k × 128 | MinIO | `cargo test -F bench bench_cold_100k_nightly -- --ignored` | [`op-scaling-100k.json`](../../benchmarks/results/op-scaling-100k.json) |
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

## 3. openpuffer measured columns (Iteration 2 — 2026-06-04)

**Host:** dev machine, MinIO via testcontainers (one healthy `openpuffer-minio-1` container; no stray MinIO cleanup needed).  
**Git at measure:** `76875cb` (pre-commit of this iteration).  
**Raw JSON:** [`op-scaling-10k.json`](../../benchmarks/results/op-scaling-10k.json), [`op-scaling-50k.json`](../../benchmarks/results/op-scaling-50k.json), [`op-scaling-100k.json`](../../benchmarks/results/op-scaling-100k.json), [`op-scaling-10k-warm.json`](../../benchmarks/results/op-scaling-10k-warm.json).

**Caveat:** 50k tier uses **v3 + release**; 10k/100k use **v2 + debug**. Cross-tier slope is not apples-to-apples until re-run at unified `ann_version` and profile.

### Cold query p50 (ms)

| Docs | Dims | Environment | p50 | p90 | p99 | ANN | Profile | \(L_{\text{norm}}\)† | vs tpuf 874 ms |
|------|------|-------------|-----|-----|-----|-----|---------|----------------------|----------------|
| 10k | 128 | minio-testcontainers | **717** | — | — | v2 | debug | 2.03M | ~2320× |
| 50k | 128 | minio-testcontainers | **377** | — | — | v3 | release | 213k | ~244× |
| 100k | 128 | minio-testcontainers | **7174** | — | — | v2 | debug | 2.03M | ~2320× |
| 500k | 128 | _optional_ | — | — | — | — | — | — | — |

† \(L_{\text{norm}} = L_{\text{op}}(N,128) \times (10^7/N) \times \sqrt{1024/128}\) per §4.3. p90/p99 not collected (7 sequential samples only).

**Per-doc cold p50 (ms/doc):** 10k → **0.072**; 50k → **0.008** (v3/release); 100k → **0.072**. 10k and 100k align on per-doc proxy; 50k is faster due to v3 + release, not doc count alone.

### Warm query p50 (ms)

| Docs | Dims | Environment | p50 | Notes |
|------|------|-------------|-----|-------|
| 10k | 128 | minio-testcontainers | **738** | [`op-scaling-10k-warm.json`](../../benchmarks/results/op-scaling-10k-warm.json); `POST /warm` + eventual, 7 runs. **Not** faster than cold (717 ms) on this MinIO/debug run — unlike tpuf warm 14 ms. |

### Preliminary scaling read (not final)

- **Doc-count shape (10k ↔ 100k, same v2/debug):** p50 grows ~10× for ~10× docs (717 → 7174 ms) → roughly **linear in N** on these two points.
- **50k point:** faster than linear trend predicts (377 ms) because **v3 probed + release**.
- **vs turbopuffer:** absolute \(L_{\text{norm}}\) ≫ 874 ms (expected: MinIO + self-host). Shape-only comparison needs unified re-runs and §4 fit — **conclusion: inconclusive** until Iteration 3.

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

- [x] Re-run / refresh `bench_cold_10k_baseline`, `bench_cold_100k_nightly`, `stress_50k` v3 cold; record p50 (p90/p99 deferred)
- [x] Add `benchmarks/results/op-scaling-{10k,50k,100k}.json` + `op-scaling-10k-warm.json`
- [x] Run `bench_cold_10k_warm_vs_cold` with stdout JSON for warm p50
- [x] Fill §3 tables; compute \(L_{\text{norm}}\) (ballpark)
- [ ] Iteration 3: unified v3 + release across tiers; p90/p99; doc-scaling \(\alpha\) fit; final conclusion

---

## 7. Related docs

- Large-tier AWS vs tpuf program: [`PLAN_LARGE_DATASET_BENCHMARK.md`](../PLAN_LARGE_DATASET_BENCHMARK.md), [`COMPARISON.md`](../COMPARISON.md)
- MinIO gates: [`BENCHMARKS.md`](../BENCHMARKS.md)
- Phase 7 exemplar reports: [`BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md`](BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md)