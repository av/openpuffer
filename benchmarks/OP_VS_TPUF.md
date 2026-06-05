# openpuffer vs turbopuffer — scaling (one page)

**Updated:** 2026-06-05 · **Artifacts through:** `adb3d38` · **Confidence:** low for extrapolation (both models); high for measured MinIO tiers.

## USER ANSWER

We measured openpuffer cold/warm query latency at **10k / 50k / 100k × 128-d** on a local **MinIO** harness and compared it to turbopuffer’s **single official** cold point at **10M × 1024-d** (874 ms p50 on GCP)—not a like-for-like benchmark. On measured tiers, openpuffer cold p50 grows **96 → 412 → 880 ms**, implying power-law **β ≈ 0.95** (near-linear doc-count scaling on this harness); turbopuffer publishes **no** (N, p50) curve, so shape similarity to tpuf is **unprovable**. The **100k ≈ 874 ms** coincidence is **not parity** (100× fewer docs, 8× fewer dims, different storage and load). Extrapolating openpuffer to **10M×128** yields **~87 s** (linear, canonical) or **~67 s** (power-law)—**~100×** or **~77×** slower than tpuf’s 874 ms, with **low confidence** because 10M was never measured. **Verdict:** openpuffer shows a **usable scaling-shape signal** on MinIO only; it does **not** scale to similar **absolute** latency or production ballpark as turbopuffer at 10M—do not treat this harness as evidence of tpuf-class performance.

---

## Did we compare?

**Yes.** openpuffer cold/warm query latency was measured at **10k / 50k / 100k × 128-d** on a local **MinIO** harness (`OPENPUFFER_ANN_VERSION=3`, release build). turbopuffer numbers are the **official fixed reference** at **10M × 1024-d** (homepage calculator + vendored `tpuf-benchmark` specs)—not re-run live (`TURBOPUFFER_API_KEY` unset).

| | openpuffer | turbopuffer |
|---|------------|-------------|
| **Source** | `benchmarks/results/op-scaling-*.json` | `benchmarks/results/tpuf-official-reference.json` |
| **Environment** | Docker MinIO testcontainers, loopback | GCP `c2-standard-30`, managed service |
| **Load** | 7 sequential cold probes / warm POST | 8 QPS × 30m sustained |
| **Live tpuf @ 10k** | — | blocked (no API key) |

---

## Does it scale similarly?

**Partially on shape, not on absolute latency.**

- **Doc-count shape (openpuffer only):** cold p50 **96 → 412 → 880 ms** (10k → 50k → 100k) implies power-law **β ≈ 0.95**—near-linear in log–log space on this harness.
- **vs turbopuffer:** only **one** official cold point at 10M exists; no published (N, p50) curve to fit against.
- **Accidental overlap:** 100k cold p50 **880 ms** ≈ tpuf **874 ms** (~**1.01×**) — **not parity** (100× fewer docs, 8× fewer dims, different backend/load).
- **10M×128 extrapolation (unmeasured, low confidence):** **linear** **~87 s** p50 (**~100×** tpuf 874 ms); **power-law** **~67 s** (**~77×**). **Canonical** = linear (`recommended_extrapolation` in summary JSON). √dim heuristic @ 10M×1024: **~247 s** (**~283×**).
- **Warm:** 10k **112 ms** (~8× tpuf 14 ms @ 10M); 100k **827 ms** (~59×)—not comparable to fleet NVMe @ 10M×1024.
- **Ingest:** openpuffer **909 / 3571 / 758 docs/s** (WAL-limited); tpuf publishes **≤~200 ms durable write-commit** (different model).

**Operator verdict:** treat results as a **scaling-shape signal** on MinIO only—**not** evidence that openpuffer matches turbopuffer at production scale.

---

## Latency table (ms, p50 / p90 / p99)

| System | Tier | Docs | Dims | Path | p50 | p90 | p99 | Notes |
|--------|------|------|------|------|-----|-----|-----|-------|
| **tpuf (official)** | 10M | 10,000,000 | 1024 | cold | **874** | 1214 | 1686 | calculator + `vector-10m-cold.toml` |
| **tpuf (official)** | 10M | 10,000,000 | 1024 | warm | **14** | 17 | 27 | `vector-10m-hot.toml` |
| **openpuffer** | 10k | 10,000 | 128 | cold | **96** | 99 | 99 | `op-scaling-10k.json` |
| **openpuffer** | 50k | 50,000 | 128 | cold | **412** | 450 | 450 | `op-scaling-50k.json` |
| **openpuffer** | 100k | 100,000 | 128 | cold | **880** | 900 | 900 | `op-scaling-100k.json` |
| **openpuffer** | 10k | 10,000 | 128 | warm | **112** | 123 | 123 | `op-scaling-10k-warm.json` |
| **openpuffer** | 100k | 100,000 | 128 | warm | **827** | 876 | 876 | `op-scaling-100k-warm.json` |
| **openpuffer (extrap., canonical)** | 10M | 10,000,000 | 128 | cold | **87321** (~87 s) | — | — | linear; **~100×** tpuf; low confidence |
| **openpuffer (extrap., alt.)** | 10M | 10,000,000 | 128 | cold | **66981** (~67 s) | — | — | power-law β≈0.95; **~77×** tpuf; low confidence |
| **openpuffer (heuristic)** | 10M | 10,000,000 | 1024 | cold | **246981** (~247 s) | — | — | √dim from 128-d tiers |

Structured summary: `benchmarks/results/scaling-comparison-summary.json` · CSV: `scaling-comparison.csv`.

---

## Reproduce at tag

The op-vs-tpuf scaling comparison program is documented for annotated tag **`bench-op-scaling-v1`** (commit range **`76875cb`..`HEAD`**). The tag is **suggested, not yet published** — `git fetch --tags` will not find it until an operator creates it (see [CHANGELOG_LARGE_DATASET.md](CHANGELOG_LARGE_DATASET.md) § Suggested tag `bench-op-scaling-v1`).

**Until the tag exists**, pin the tree at:

```bash
git checkout dc893f2   # last known green verify for the scaling program
# or
git checkout HEAD      # latest committed op-scaling artifacts and docs
```

**When `bench-op-scaling-v1` is published:**

```bash
git fetch --tags
git checkout bench-op-scaling-v1
make bench-verify-op-scaling   # offline gate only
```

Use the commands below to **remeasure** tiers (slow; Docker MinIO). For verify/compare only, stay on the pinned commit and run steps **2** and **3**.

---

## Reproduce (3 commands)

```bash
# 1. Measure tiers (skip if committed op-scaling-*.json already present)
make bench-op-scaling

# 2. Compare vs tpuf reference + write scaling-comparison-summary.json
make bench-compare-tpuf

# 3. Offline CI gate (schema + compare smoke; no Docker)
make bench-verify-op-scaling
```

**Offline-only** (no remeasure): run **2** and **3**. One-line verdict: `./scripts/print-scaling-verdict.sh`. Full quickstart: [`SCALING_VS_TPUF_QUICKSTART.md`](SCALING_VS_TPUF_QUICKSTART.md).

---

## Full report

- **User report (tables, models, warm/ingest):** [`docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md`](../docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md)
- **Iteration log / confidence matrix:** [`docs/reports/OPENS_VS_TPUF_SCALING_COMPARISON.md`](../docs/reports/OPENS_VS_TPUF_SCALING_COMPARISON.md)

---

## Disclaimer (MinIO vs managed)

These numbers **do not** predict openpuffer on AWS/GCP or turbopuffer on your laptop.

| Factor | openpuffer harness | turbopuffer reference |
|--------|---------------------|------------------------|
| Storage | MinIO (local/container) | Managed object store + ANN fleet |
| Scale measured | ≤ **100k** docs | **10M** docs (official) |
| Dimensions | **128** synthetic | **1024** Cohere embeddings |
| Query load | 7 samples, cold cache flush | 8 QPS × 30 minutes |
| ANN v3 blog (100B) | not comparable | p99/QPS targets only—no doc-count p50 curve |

Do **not** cite **100k ≈ 874 ms** as production parity. Extrapolation to 10M is **unmeasured**; linear (~87 s) and power-law (~67 s) both **low confidence**—use **linear** as canonical only for reporting consistency.