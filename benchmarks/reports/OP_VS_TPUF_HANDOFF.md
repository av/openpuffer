# openpuffer vs turbopuffer scaling — operator handoff

**Date:** 2026-06-05  
**Program:** Timeboxed op-vs-tpuf scaling comparison (`76875cb` … `0e3ba2f`, ~55 commits)  
**Status:** **Offline MinIO comparison COMPLETE** · **Live like-for-like PENDING** (AWS S3 + `TURBOPUFFER_API_KEY`)  
**Final verify (session end):** `./scripts/verify-op-scaling-comparison.sh` exit **0** @ **`0e3ba2f`** (offline; no MinIO remeasure)

---

## TL;DR

Measured openpuffer cold/warm query latency at **10k / 50k / 100k × 128-d** on a local **MinIO** harness and compared to turbopuffer’s **single official** point at **10M × 1024-d** (874 ms p50, GCP). This is **not** a like-for-like benchmark.

| Track | State |
|-------|--------|
| MinIO op-scaling tiers + compare tooling + CI smoke | **Done** — `make bench-verify-op-scaling` exit **0** |
| Live tpuf @ 10k / openpuffer @ AWS @ 10M | **Blocked** — no API key; no EC2/AWS JSON |

**One command (offline, no Docker remeasure):**

```bash
make bench-compare-tpuf && make bench-verify-op-scaling
./scripts/print-scaling-verdict.sh
```

---

## What was done (timebox)

| Area | Deliverable |
|------|-------------|
| **Measurements** | `benchmarks/results/op-scaling-{10k,50k,100k,10k-warm,100k-warm,10k-synthetic128}.json` |
| **tpuf reference** | `benchmarks/results/tpuf-official-reference.json` (10M×1024 official cold/warm + ann-v3 blog cross-ref) |
| **Derived** | `scaling-comparison-summary.json`, `scaling-comparison.csv` (linear + power-law fits) |
| **Compare** | `scripts/compare-op-scaling-to-tpuf.sh`, `benchmarks/report/compare_op_scaling_to_tpuf.py`, `scripts/print-scaling-verdict.sh` |
| **Harness** | `scripts/run-op-scaling-benchmark.sh` (cold + warm tiers, ingest metrics, p90/p99) |
| **CI** | `scripts/verify-op-scaling-comparison.sh`, nightly smoke, shellcheck, 100k outlier gate |
| **Docs** | [`benchmarks/OP_VS_TPUF.md`](../OP_VS_TPUF.md), [`docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md`](../../docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md), quickstart + FAQ |

**Progress log:** `/tmp/timeboxed-op-vs-tpuf-comparison-1780613123.md`

---

## USER ANSWER: does openpuffer scale like turbopuffer?

**Nuanced — no for absolute production ballpark; partial yes for doc-count shape on MinIO only.**

| Question | Answer |
|----------|--------|
| Same absolute latency @ 10M? | **No** — linear extrap **87,321 ms** vs tpuf **874 ms** (**~100×**); power-law **~67 s** (**~77×**); low confidence (unmeasured @ 10M) |
| Similar doc-count scaling *shape* on op harness? | **Partial** — cold p50 **96 → 412 → 880 ms** (β≈0.95); tpuf has **no** (N, p50) curve |
| 100k ≈ 874 ms coincidence? | **Not parity** — 100× fewer docs, 8× fewer dims, MinIO vs GCP fleet |
| Warm path? | **No** — 10k **112 ms**, 100k **827 ms** vs tpuf **14 ms** @ 10M |

**Verdict:** usable **scaling-shape signal** on MinIO only; **not** tpuf-class absolute latency at 10M. Do not treat this harness as production parity evidence.

---

## How to reproduce

### Offline (committed JSON — recommended)

```bash
make bench-compare-tpuf          # regen summary from committed op-scaling + tpuf ref
make bench-verify-op-scaling     # schema, compare smoke, facts (no Docker)
./scripts/print-scaling-verdict.sh
```

### Remeasure tiers (slow; Docker MinIO)

```bash
make bench-op-scaling            # 10k / 50k / 100k cold (+ optional warm tiers)
make bench-compare-tpuf
make bench-verify-op-scaling
```

Full steps: [`benchmarks/SCALING_VS_TPUF_QUICKSTART.md`](../SCALING_VS_TPUF_QUICKSTART.md) · one-page: [`benchmarks/OP_VS_TPUF.md`](../OP_VS_TPUF.md).

---

## Blocked: true like-for-like comparison (AWS + API key)

MinIO @ ≤100k×128-d **cannot** validate openpuffer vs turbopuffer at production scale. Remaining work uses the **large-dataset harness** (sibling program):

| Requirement | Why blocked | Operator fix |
|-------------|-------------|--------------|
| **AWS S3 + EC2** | op-scaling used MinIO testcontainers only | `scripts/run-aws-large-benchmark.sh --tier l1` on EC2 in bucket region |
| **`TURBOPUFFER_API_KEY`** | Live tpuf @ 10k skipped; no `tpuf-scaling-10k-live.json` | Test-org key; `scripts/run-tpuf-large-benchmark.sh --tier l1` |
| **Same workload @ L1** | op-scaling ≠ apples-to-apples with tpuf 10M official row | Shared `synthetic-128` L1 (100k×128) per [PLAN_LARGE_DATASET_BENCHMARK.md](../../docs/PLAN_LARGE_DATASET_BENCHMARK.md) |
| **Measured G5 report** | scaling report extrapolates; does not replace live JSON | `scripts/run-large-benchmark-program.sh --tier l1 --measured-report` |

**Sibling handoff:** [`docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md`](../../docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md)

**Checklist for “true” comparison:**

- [ ] `benchmarks/results/large-aws-l1.json` (`environment=aws-s3`)
- [ ] `benchmarks/results/tpuf-l1.json` (`environment=turbopuffer:<region>`)
- [ ] Measured `docs/reports/BENCHMARK_VS_TURBOPUFFER_<date>.md` + [COMPARISON.md](../../docs/COMPARISON.md) L1 rows
- [ ] Optional: live tpuf scaling @ 10k with API key (`tpuf-scaling-10k-live.json`)

---

## Key documentation

| Document | Purpose |
|----------|---------|
| [`benchmarks/OP_VS_TPUF.md`](../OP_VS_TPUF.md) | One-page answer + reproduce |
| [`docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md`](../../docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md) | Full tables, models, warm/ingest |
| [`docs/reports/OPENS_VS_TPUF_SCALING_COMPARISON.md`](../../docs/reports/OPENS_VS_TPUF_SCALING_COMPARISON.md) | Iteration log |
| [`benchmarks/SCALING_VS_TPUF_QUICKSTART.md`](../SCALING_VS_TPUF_QUICKSTART.md) | Operator quickstart |
| [`docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md`](../../docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md) | AWS/tpuf live measurement path |

---

## Reviewer sign-off

| Question | Answer |
|----------|--------|
| Is the offline comparison safe to merge? | Yes, if `verify-op-scaling-comparison.sh` exits 0 |
| Does openpuffer match tpuf @ 10M from this work? | **No** (~100× extrapolated cold p50; low confidence) |
| Can we claim 100k ≈ 874 ms parity? | **No** — accidental overlap only |
| What unblocks production-grade answer? | EC2 + real S3 + tpuf test key → large-dataset L1 program |

**Handoff author:** timeboxed op-vs-tpuf session (`/tmp/timeboxed-op-vs-tpuf-comparison-1780613123.md`). **Next owner:** operator with AWS/tpuf credentials per large-dataset runbook.