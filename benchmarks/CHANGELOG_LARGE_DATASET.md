# Large-dataset benchmark harness — changelog

Chronological record of commits that built the **offline harness** for [PLAN_LARGE_DATASET_BENCHMARK.md](../docs/PLAN_LARGE_DATASET_BENCHMARK.md) (A1–A6, G2–G6, JSON schemas, operator wrappers). **Live G3/G4 JSON and measured G5** are still operator-pending; see [results/OPERATOR_G3_G4_ATTEMPT.md](results/OPERATOR_G3_G4_ATTEMPT.md).

**Verify gate:** `./scripts/verify-large-benchmark-program.sh` (exit **0** @ `779508a`, 2026-06-04T03:47:30Z; facts 35+18 passed, G2 optional `--with-g2`)

**Milestone tag:** `large-dataset-harness-v1` → `c7f66a3` (annotated, 2026-06-04) — offline harness complete before post-tag doc polish (iterations 98–113)

**Session log:** timeboxed run 2026-06-04 (**114** iterations); progress file `/tmp/timeboxed-large-dataset-benchmark-plan-1780525473.md`

**Op-vs-tpuf scaling program (2026-06-04 → 2026-06-05):** timeboxed comparison through `31dfc8e`; progress `/tmp/timeboxed-op-vs-tpuf-comparison-1780613123.md`

---

## Session 2026-06-05 — openpuffer vs turbopuffer scaling comparison

| Field | Value |
|-------|--------|
| **Goal** | Compare openpuffer MinIO cold scaling (10k / 50k / 100k × 128) to turbopuffer official **874 ms** @ **10M × 1024** (GCP); deliver committed JSON, scripts, extrapolation, and operator report |
| **Commits** | `76875cb` … `31dfc8e` (measurements, compare/verify gates, CI smoke, docs, warm tiers, ingest/efficiency analysis, outlier gate) |
| **Reproduce** | [SCALING_VS_TPUF_QUICKSTART.md](SCALING_VS_TPUF_QUICKSTART.md) — 5-command MinIO sweep + offline compare/verify |
| **Verify gate** | `make bench-verify-op-scaling` → `./scripts/verify-op-scaling-comparison.sh` (offline; committed `op-scaling-*.json` + `tpuf-official-reference.json`) |
| **CI** | `.github/workflows/ci.yml` job `op-scaling-comparison`; `verify-large-benchmark-program.sh --skip-op-scaling` avoids duplicate work |
| **User report** | [BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md](../docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md); hub [OPENS_VS_TPUF_SCALING_COMPARISON.md](../docs/reports/OPENS_VS_TPUF_SCALING_COMPARISON.md) |

### Key commits (`76875cb` → `31dfc8e`, oldest first)

- `76875cb` — tpuf official reference + scaling comparison skeleton
- `256d46e` — openpuffer scaling measurements for tpuf comparison
- `382fbda` — unified op scaling runs and tpuf comparison conclusion
- `39d4d17` — extrapolate openpuffer scaling to tpuf 10M reference
- `7142ba2` — harden op-vs-tpuf scaling comparison tooling
- `7769c14` — validate op scaling extrapolation models
- `464ac91` — CI smoke gate for op-vs-tpuf scaling comparison
- `da45441` — publish openpuffer vs tpuf scaling comparison report 2026-06-04
- `9c637d1` — refresh op-scaling measurement JSON (full tier sweep)
- `21ccd12` — reconcile tpuf scaling comparison after 100k remeasure
- `df431b3` — efficiency metrics in tpuf scaling comparison
- `ad8af56` — charts and quickstart for op-vs-tpuf scaling
- `aad774e` — facts gates for op-vs-tpuf scaling comparison
- `7f7c0f5` — p90/p99 and ingest throughput in op-scaling JSON
- `6776874` — refresh 100k op-scaling with ingest metrics
- `2738d6f` — canonical extrapolation model for tpuf comparison
- `a2c1fd6` — 100k latency vs tpuf 10M reference insight (coincidental ms, not parity)
- `ff47227` — changelog for op-vs-tpuf scaling comparison program
- `b9b793e` — warm scaling tier for tpuf comparison
- `979e2d6` — warm metrics in tpuf scaling compare script
- `b7dac2b` — `scaling-comparison-summary.json` artifact
- `fecec4f` — verify integration for op-vs-tpuf scaling
- `82930ab` — dimension scaling sensitivity for tpuf comparison
- `afc48fa` — CSV export for scaling comparison
- `4a2f839` — optional op-scaling refresh CI workflow
- `032a5ef` — sync scaling comparison executive summaries
- `e67a936` — facts: mark op-vs-tpuf scaling comparison implemented
- `88df45a` — automate 100k vs tpuf order-of-magnitude gate
- `03f71c1` — FAQ for turbopuffer scaling comparison
- `dcb46a7` — ingest throughput vs tpuf write claims
- `31dfc8e` — gate op-scaling 100k outlier detection

### How to reproduce

See **[SCALING_VS_TPUF_QUICKSTART.md](SCALING_VS_TPUF_QUICKSTART.md)** (`make bench-op-scaling` → `make bench-compare-tpuf` → `make bench-verify-op-scaling`). Offline gate only needs committed `benchmarks/results/op-scaling-*.json` + `tpuf-official-reference.json`.

### Artifacts and scripts

| Artifact | Role |
|----------|------|
| `benchmarks/results/op-scaling-{10k,50k,100k}.json` | Cold p50/p90/p99 from 7 `query_latencies_ms`; `ingest_wall_secs` / `docs_per_sec` |
| `benchmarks/results/op-scaling-10k-warm.json` | Warm path @ 10k (not tpuf-comparable without matched cache tier) |
| `benchmarks/results/op-scaling-10k-synthetic128.json` | synthetic-128 G2 gate @ 10k (≥3 cold tiers for fit) |
| `benchmarks/results/tpuf-official-reference.json` | Official tpuf calculator / tpuf-benchmark reference (**874 ms** cold p50 @ 10M × 1024) |
| `scripts/run-op-scaling-benchmark.sh` | Regenerate all tiers (`make bench-op-scaling`) |
| `scripts/compare-op-scaling-to-tpuf.sh` | `compare_op_scaling_to_tpuf.py` → `EXTRAP_JSON` + verdict |
| `scripts/verify-op-scaling-comparison.sh` | Schema, facts, compare smoke (offline gate) |
| `benchmarks/report/schema/op-scaling.schema.json` | `op_scaling_v1` JSON Schema |

### Measured cold p50 (committed @ `7f7c0f5` refresh; reconciled in final sweep @ `a2c1fd6`)

| Tier | p50 / p90 / p99 (ms) | Ingest | Notes |
|------|----------------------|--------|--------|
| 10k × 128 | **96 / 99 / 99** | 11 s @ **909 docs/s** | `bench_cold_10k_baseline` |
| 50k × 128 | **412 / 450 / 450** | 14 s @ **3571 docs/s** | `stress_50k` v3 cold probed |
| 100k × 128 | **880 / 900 / 900** | 132 s @ **758 docs/s** | `bench_cold_100k_nightly`; `index_object_count` ~280 on bench log |
| 10k warm | **112 ms** p50 (final sweep; was **81 ms**) | — | `bench_cold_10k_warm_vs_cold` |
| 10k synthetic-128 | **97 ms** p50 | — | G2 workload gate |

### Canonical extrapolation vs tpuf (linear model, `2738d6f` + `compare_op_scaling_to_tpuf.py`)

- **Best fit among tried models:** log-linear / √dim heuristics; **linear in doc count @ 128-d** is the canonical comparison (`canonical_model: linear`).
- **Extrapolated openpuffer p50 @ 10M × 128:** **~87 s** (~**87321 ms** in `EXTRAP_JSON`; ~**100×** vs tpuf **874 ms** if MinIO scaling holds to 10M).
- **Insight (`a2c1fd6`):** measured **880 ms** @ **100k × 128** is **coincidentally** near tpuf **874 ms** @ **10M × 1024**—different **N**, **D**, and backend; **not** parity. Do not read 100k ≈ 874 ms as “same ballpark.”
- **Live tpuf @ 10k:** blocked without `TURBOPUFFER_API_KEY` (`tpuf-scaling-10k-live.json` pending/skipped).

### Makefile / tests

- `make bench-op-scaling`, `make bench-compare-tpuf`, `make bench-verify-op-scaling`
- `scripts/test_compare-op-scaling-to-tpuf.sh`, `scripts/test_validate-op-scaling-json.sh`
- Smoke: `cargo test -F bench bench_cold_10k_baseline -q` (10k cold harness)

### Program status

**Offline comparison program complete** through `31dfc8e`: committed measurements, extrapolation tooling, verify gate, CI smoke, warm/ingest/dimension analysis, 100k outlier gate, and published scaling report. **Live** tpuf re-measure at matched tiers remains operator-pending (API key / EC2).

---

## Session 2026-06-04

| Field | Value |
|-------|--------|
| **Time window** | 2026-06-04 00:24 CEST → ~06:00 CEST (timeboxed PLAN_LARGE_DATASET_BENCHMARK implementation) |
| **Total iterations** | **114** (A1–A6 + G2–G6 harness build, polish, milestone tag, doc closure; see tables below) |
| **Harness commits** | `6186190` … `779508a` (offline program complete; tag @ `c7f66a3`) |
| **Verify gate** | `./scripts/verify-large-benchmark-program.sh` exit **0** @ **`779508a`** (2026-06-04T03:47:30Z; optional `--with-g2` for MinIO Docker parity) |
| **Milestone tag** | `git checkout large-dataset-harness-v1` → **`c7f66a3`** |

### Harness complete

All **offline** checklist rows for [PLAN_LARGE_DATASET_BENCHMARK.md](../docs/PLAN_LARGE_DATASET_BENCHMARK.md) are satisfied: synthetic workload (A1), ingest/bench/tpuf drivers (A2–A4), report merge + measured renderer (A5), CI dispatch + nightly dry-run (A6), G2 MinIO correctness gates, G3/G4 operator wrappers + preflights, G5 schema/exemplar JSON + `fill-comparison-from-report.sh`, G6 regression wiring, JSON schemas L1–L3, id-overlap mock, SLO gate scripts (`check-large-aws-gates`, `check-tpuf-gates`), and the unified verify gate.

**Statement:** The large-dataset benchmark **harness is complete** and safe for operator handoff / merge. `./scripts/verify-large-benchmark-program.sh` is the single offline gate; last full audit passed at `779508a` (2026-06-04T03:47:30Z, session-end verify before 6am deadline).

**Milestone:** Annotated tag **`large-dataset-harness-v1`** on `c7f66a3` marks the offline harness baseline (pinned Python deps). Commits `d4a96f1` … `779508a` are documentation and operator UX only — no harness behavior change required before live G3–G5.

Operator PR summary: [LARGE_DATASET_HARNESS_HANDOFF.md](../docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md). Archived phase spec: [PLAN_LARGE_DATASET_BENCHMARK_PHASES_1-7.md](../docs/archive/PLAN_LARGE_DATASET_BENCHMARK_PHASES_1-7.md).

### Live work remaining

**Program complete** (measured comparison + publish) is **not** done — blocked on operator credentials and EC2 only (not harness gaps):

| Step | Artifact / outcome | Blocker on dev host |
|------|-------------------|---------------------|
| G3 | `benchmarks/results/large-aws-l1.json` (+ ingest sidecar) | MinIO-style endpoint; needs EC2 + real AWS S3 |
| G4 | `benchmarks/results/tpuf-l1.json` | `TURBOPUFFER_API_KEY` unset |
| Phase 3.3 | `benchmarks/results/id-overlap-l1.json` | Requires live G3 + G4 namespaces |
| G5 | Measured dated report + [COMPARISON.md](../docs/COMPARISON.md) L1 rows | Depends on live JSON above |
| @spec | Live `large-aws-l1` / `tpuf-l1` facts (placeholders exist @ `af43e43`) | Same as G3/G4 |

**Do not** re-run live G3/G4 on the MinIO dev host. Use [OPERATOR_RUNBOOK_QUICK.md](OPERATOR_RUNBOOK_QUICK.md) on bench EC2; blocked attempts: [results/OPERATOR_G3_G4_ATTEMPT.md](results/OPERATOR_G3_G4_ATTEMPT.md).

**Operator sequence (L1):** `preflight-large-benchmark-all.sh` (or `make bench-preflight`) → G3 `run-aws-large-benchmark.sh` → G4 `run-tpuf-large-benchmark.sh` → `run-id-overlap-spotcheck.sh` → SLO gates → `run-large-benchmark-program.sh --measured-report` (or `render-report.sh` + `fill-comparison-from-report.sh`) → [OPERATOR_AFTER_LIVE_RUN.md](OPERATOR_AFTER_LIVE_RUN.md) for `@spec` `8zb`/`7ow`.

---

## Epilogue — iterations 85–114 (2026-06-04)

Post–iteration-84 polish: SLO gates, PLAN archive, milestone tag, unified preflight, CI dispatch preflight, link/anchor sweeps, and session closure. **Live G3–G5 unchanged** — still EC2 + AWS S3 + `TURBOPUFFER_API_KEY`.

| Iter | Commit | Description |
|------|--------|-------------|
| **85** | `7e1f876` | id-overlap live mode: clearer empty-namespace errors + preflight |
| **86** | `8480ccb` | `render-report.sh` partial merge when only large-aws or only tpuf JSON present |
| **87** | `d516003`, `14f0980`, `161ee75` | SLO gate scripts (`check-large-aws-gates`, `check-tpuf-gates`); wired into program chain |
| **88** | `7a38d7f` | L2/L3 `render-report --dry-run` falls back to tier `*.example.json` |
| **89** | `3b99883` | HANDOFF / CHANGELOG: verify gate recorded @ `7a38d7f` |
| **90** | `38ca7c6` | `fill-comparison-from-report.sh` — populate COMPARISON.md L1 from measured G5 |
| **91** | `5dd8175` | Optional Prometheus scrape during `bench-large` (BENCHMARKS § Phase 5.2; not in comparison JSON) |
| **92** | `f7643e8` | Archive PLAN Phases 1–7; main PLAN Phase 8 harness table; fact counts 34/18 |
| **93** | `1dc58b2` | Session 2026-06-04 summary + iterations 75–90 table (this file) |
| **94** | `49c3431` | Harness TODO sweep complete — no offline PLAN `[ ]` before live G3–G5 |
| **95** | `69199ab` | HANDOFF scope audit: harness/scripts/docs/API only; no UI assets |
| **96** | `c7f66a3` | Pin `jsonschema` + `turbopuffer` in `benchmarks/requirements.txt` |
| **97** | `8a56cbd` | Annotated tag **`large-dataset-harness-v1`** @ `c7f66a3`; HANDOFF checkout instructions |
| **98** | `d4a96f1` | Verify gate exit **0** @ `8a56cbd` (post-tag HEAD; 34 + 18 facts) |
| **99** | `c160a4b` | `bench_cold` module docs ↔ large-dataset program + synthetic-128 |
| **100** | `13e0593` | Fix broken relative links in PLAN + `benchmarks/README.md` |
| **101** | `20e9a41` | BENCHMARKS/COMPARISON `--` anchor slug fixes |
| **102** | `d4ac309` | Secrets + tpuf README BENCHMARKS anchor fixes |
| **103** | `b6dcae1` | Archive PLAN Phases 1–7: G3 BENCHMARKS anchor fix |
| **104** | `dcf1b9a` | `preflight-large-benchmark-all.sh` — G3/G4/overlap bundle |
| **105** | `7cd54ef` | Main PLAN Phases 4–6 BENCHMARKS anchor fixes |
| **106** | `71f2175` | `make bench-preflight` → unified preflight bundle |
| **107** | `7780e5a` | A6 `benchmark-large-dispatch`: preflight before verify (`--skip-overlap --tier`) |
| **108** | `2bf2dce` | [OPERATOR_AFTER_LIVE_RUN.md](OPERATOR_AFTER_LIVE_RUN.md) — activate `@spec` `8zb`/`7ow` after live JSON |
| **109** | — | Verify exit **0** @ `2bf2dce` (18 passed; G2 skipped) |
| **110** | — | Progress log sync (iterations 85–108) |
| **111** | — | Session closure @ `2bf2dce`: harness complete; live G3–G5 pending |
| **112** | `5f03f2b` | HANDOFF: optional `verify-large-benchmark-program.sh --with-g2` for Docker G2 |
| **113** | `a779510` | Root README: expanded large-dataset benchmark section |
| **114** | — | CHANGELOG epilogue 85–114; session metadata + `large-dataset-harness-v1` tag reference |

### Tag `large-dataset-harness-v1`

```bash
git fetch --tags
git checkout large-dataset-harness-v1   # → c7f66a3
git show large-dataset-harness-v1       # annotated message
```

Tag marks **offline harness complete** (A1–A6, G2, G3/G4 operators, G5 renderer, G6 CI/nightly, verify gate, schemas, id-overlap mock). **Not included:** measured `large-aws-l1.json`, `tpuf-l1.json`, id-overlap live JSON, or publishable G5/COMPARISON rows. Post-tag commits are doc/operator polish only.

**Checkout for live run:** use `main` @ `779508a` or later for [OPERATOR_RUNBOOK_QUICK.md](OPERATOR_RUNBOOK_QUICK.md) + [OPERATOR_AFTER_LIVE_RUN.md](OPERATOR_AFTER_LIVE_RUN.md); use tag when bisecting harness baseline.

---

## Iterations 75–90 (session closure)

| Iter | Commit | Description |
|------|--------|-------------|
| **75** | `e86a010` | CHANGELOG iterations 49–74 table |
| **76** | `8ca2eb4` | Sync `@spec` bench-large / bench-tpuf fact counts (32/15) |
| **77** | `f30f46c` | HANDOFF: offline harness — no remaining gaps |
| **78** | `4da0713` | Operator docs typo sweep (paths, tiers, fact counts) |
| **79** | `a103ec0` | G2 MinIO `docker-compose.test.yml` operator docs |
| **80** | `fbddf10` | Nightly `bench-100k` ↔ large-dataset program link (G6) |
| **81** | `af43e43` | `@spec` placeholders for live `large-aws-l1` / `tpuf-l1` JSON |
| **82** | `387b7d0` | HANDOFF verify gate recorded @ `af43e43` |
| **83** | `7e1f876` | id-overlap live mode errors + preflight hardening |
| **84** | `8480ccb` | `render-report.sh` partial merge when one JSON side missing |
| **85** | `d516003`, `14f0980`, `161ee75` | SLO gate scripts + wire into `run-large-benchmark-program.sh` |
| **86** | `7a38d7f` | L2/L3 `render-report --dry-run` uses tier `*.example.json` |
| **87** | `3b99883` | Final verify gate documentation @ `7a38d7f` |
| **88** | `38ca7c6` | `fill-comparison-from-report.sh` for COMPARISON L1 publish |
| **89** | `5dd8175` | Optional Prometheus scrape during `bench-large` (BENCHMARKS) |
| **90** | `f7643e8` | Archive PLAN Phases 1–7; Phase 8 harness status table |

---

## Iterations 49–74 (session 2026-06-04)

| Iter | Commit | Description |
|------|--------|-------------|
| **49** | `5aca279` | G3 live retry skipped on MinIO; add [OPERATOR_RUNBOOK_QUICK.md](OPERATOR_RUNBOOK_QUICK.md) EC2 one-pager |
| **50** | `aa7de00` | `large-benchmark-serve-ready.sh` — poll `/health` or `/v1/ready`; wired into ingest-large + bench-large |
| **51** | `cf5b490` | This changelog — harness commit history for operator handoff |
| **52** | `622a1b8` | `GET /v1/ready` S3 deep probe (Rust API); integration test; serve-ready prefers `/v1/ready` |
| **53** | `fe6fc92` | Mandate `OPENPUFFER_ANN_VERSION=3`; validate `preferred_ann_version == 3`; G2 serve `--ann-version 3` |
| **54** | `64654dd` | Large-benchmark cost estimator script for L1–L3 operator planning |
| **55** | `4459e3a` | Skip `run-tpuf-large-benchmark.sh` when `TURBOPUFFER_API_KEY` unset; log in `OPERATOR_G3_G4_ATTEMPT.md` |
| **56** | `9670556` | `.gitignore` live tier JSON; `check-benchmark-artifacts.sh` in verify gate; README staging rules |
| **57** | `9d5b87b` | Harness audit: `verify-large-benchmark-program.sh` exit 0; PLAN harness-complete statement |
| **58** | `f712750` | Fix harness audit changelog commit reference |
| **59** | `43fa706` | PLAN architecture mermaid — real script names and operator sequence |
| **60** | — | Session status: harness complete @ `43fa706`; verify PASS @ `9670556`; live pending EC2 + S3 + tpuf key |
| **61** | `57f7f39` | Index timeout troubleshooting in `docs/BENCHMARKS.md`; `scripts/diagnose-index-lag.sh` |
| **62** | `29f50fb` | `schema_version: large_benchmark_v1` on ingest/bench/tpuf/id-overlap JSON + schemas + validate gate |
| **63** | `2b7984b` | `.gitattributes` + `normalize-benchmark-json.sh` for diff-friendly benchmark JSON; CI `--check` |
| **64** | `51a6f79` | Makefile `bench-verify` / `bench-dry-run` / `bench-g2-minio`; README links |
| **65** | `f3103c8` | [workloads/QUERY_SPEC.md](workloads/QUERY_SPEC.md) — `queries.json` structure, cold/warm, recall_defaults |
| **66** | `1b5fb41` | Consolidate benchmark Python deps → `benchmarks/requirements.txt`; `install-benchmark-python-deps.sh` |
| **67** | `ae22a7a` | Secret-echo audit gate and preflight hardening for operator scripts |
| **68** | `3c2ca6e` | Document ingest-large sequential batches (WAL + index lag) |
| **69** | `756f1a8` | ISO8601 UTC timestamps (`Z` suffix) on harness result JSON; `utc_timestamps.py` + validate gate |
| **70** | `04c50f1` | [workloads/EMBEDDINGS.md](workloads/EMBEDDINGS.md) — `bench_sin_v1` vs `xorshift_f32` |
| **71** | `84bb89a` | [LARGE_DATASET_HARNESS_HANDOFF.md](../docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md) operator PR summary |
| **72** | `f123909` | CI: pin Python 3.11 for benchmark workflows and verify gate |
| **73** | `a061bcd` | Standard exit codes documented for large-dataset benchmark scripts |
| **74** | `02398a8`, `d09192d` | shellcheck fixes for exit-codes and serve-ready libs; live G3/G4 attempt 3 blocked (MinIO, no tpuf key) |

---

## By program track

| Track | Commits | One-line summary |
|-------|---------|------------------|
| **A1** workload | `6186190`, `76ff071` | Deterministic `generate_synthetic.py` + committed L1/L2/L3 manifests and queries |
| **A2** ingest | `d788b65`, `98a9ff7`, `d0bc8ba`, `5da0aa1`, `aa7de00`, `3c2ca6e` | `ingest-large.sh` with timing JSON, S3 retry/resume, serve readiness, batch/WAL docs |
| **A3** bench | `9272b8f`, `f877d64`, `5ccf9eb` | `bench-large.sh` tiered cold/filter/hybrid/warm → `large-aws-{tier}.json` |
| **A4** tpuf driver | `08e66ce`, `696a247`, `5da0aa1` | `run_benchmark.py` ingest + cold/filter/hybrid/warm + recall; ingest retry parity |
| **A5** report | `59f5822`, `265c635`, `bd449b6` | `render-report.sh` merge; measured-mode schema/interpretation/redaction; exemplar + MinIO shape JSON |
| **A6** CI dispatch | `1902c62`, `58c3271`, `f123909` | `benchmark-large-dispatch.yml` dry-run; live secrets doc; Python 3.11 pin |
| **API** readiness | `622a1b8`, `aa7de00` | `GET /v1/ready` + shared serve-ready poll before ingest/bench |
| **G2** correctness | `5972ab7`, `67c7050`, `ef4fa97`, `2cccc7e`, `33a14d1`, `476a753`, `fe6fc92` | MinIO gates, full filter/hybrid queries, CI `g2-minio-correctness`, 10k schema path, ANN v3 mandate |
| **G3** AWS operator | `a1b34cc`, `ee30fd1`, `5aca279`, `64654dd` | AWS wrapper, EC2 preflight/runbook, cost estimator (live blocked on MinIO dev host) |
| **G4** tpuf operator | `95197a9`, `fa08a69`, `4459e3a` | tpuf harness, preflight, skip when API key unset |
| **Phase 3.3** overlap | `52e3208`, `3a3904a` | Cross-engine id spot-check + JSON Schema + production-shaped mock |
| **E2E** orchestration | `250257b`, `bfaec74`, `a309b8d`, `51a6f79` | Program chain, verify gate, L2/L3 timeouts, Makefile targets |
| **JSON schemas** | `c91c063`, `fe7edd3`, `5627247`, `29f50fb`, `756f1a8`, `2b7984b` | Schemas L1–L3, `schema_version`, UTC timestamps, normalize + gitattributes |
| **G6** regression | `433d2fd`, `833e1bc`, `9d5b87b` | Nightly program dry-run; harness audit; MinIO L1 schema example |
| **@spec** facts | `77a45bb`, `2337cbb` | `bench-large` / `bench-tpuf` tags; validate, preflight, schema facts |
| **Docs / hub** | `8594099`, `23fe932`, `3ddf464`, `4a7102a`, `7424cad`, `32cb61c`, `cf5b490`, `43fa706`, `f3103c8`, `04c50f1`, `84bb89a`, `57f7f39` | Runbooks, README, PLAN/COMPARISON, QUERY_SPEC, EMBEDDINGS, handoff PR summary |
| **Harness hygiene** | `868fd26`, `ff3eef8`, `02398a8`, `ae22a7a`, `a061bcd`, `1b5fb41` | shellcheck, exit codes, secret-echo audit, consolidated Python deps |
| **Operator attempts** | `32cb61c`, `5aca279`, `4459e3a`, `d09192d` | Logged G3/G4 live skips (MinIO env, no tpuf key) |
| **SLO gates** | `d516003`, `14f0980`, `161ee75` | `check-large-aws-gates.sh`, `check-tpuf-gates.sh`; wired into program chain |
| **Publish helpers** | `38ca7c6`, `8480ccb`, `7e1f876` | `fill-comparison-from-report.sh`; partial render merge; id-overlap live UX |
| **Session closure** | `7a38d7f`, `3b99883`, `f7643e8`, `5dd8175`, `af43e43`, `387b7d0` | Final verify @ `7a38d7f`; PLAN archive; Prometheus doc; live @spec placeholders |
| **Milestone tag** | `c7f66a3`, `8a56cbd` | Pin benchmark deps; annotated `large-dataset-harness-v1` |
| **Post-tag polish** | `d4a96f1` … `779508a` | Verify records, link/anchor sweeps, unified preflight, CI preflight, OPERATOR_AFTER_LIVE_RUN, README, session-end verify |
| **Epilogue** | — | CHANGELOG iterations 85–114 table + tag reference (iter 114) |

---

## All commits (newest first)

| Date | Commit | Description |
|------|--------|-------------|
| 2026-06-04 | — | CHANGELOG epilogue iterations 85–114; session metadata @ 114 iterations |
| 2026-06-04 | `a779510` | Root README: expanded large-dataset benchmark section |
| 2026-06-04 | `5f03f2b` | HANDOFF: optional verify `--with-g2` for Docker MinIO G2 |
| 2026-06-04 | `2bf2dce` | [OPERATOR_AFTER_LIVE_RUN.md](OPERATOR_AFTER_LIVE_RUN.md) — post-live `@spec` `8zb`/`7ow` |
| 2026-06-04 | `7780e5a` | CI: `preflight-large-benchmark-all` in `benchmark-large-dispatch` |
| 2026-06-04 | `71f2175` | Makefile `bench-preflight` target |
| 2026-06-04 | `7cd54ef` | PLAN Phases 4–6 BENCHMARKS anchor fixes |
| 2026-06-04 | `dcf1b9a` | `preflight-large-benchmark-all.sh` G3/G4/overlap bundle |
| 2026-06-04 | `b6dcae1` | Archive PLAN: G3 BENCHMARKS anchor fix |
| 2026-06-04 | `d4ac309` | Secrets + tpuf README BENCHMARKS anchor fixes |
| 2026-06-04 | `20e9a41` | BENCHMARKS/COMPARISON double-hyphen anchor fixes |
| 2026-06-04 | `13e0593` | PLAN + benchmarks README relative link fixes |
| 2026-06-04 | `c160a4b` | `bench_cold` module docs ↔ large-dataset program |
| 2026-06-04 | `d4a96f1` | Verify gate @ `8a56cbd` (post-tag) |
| 2026-06-04 | `8a56cbd` | Record `large-dataset-harness-v1` tag in HANDOFF |
| 2026-06-04 | `c7f66a3` | Pin jsonschema + turbopuffer in `benchmarks/requirements.txt` |
| 2026-06-04 | `69199ab` | HANDOFF scope audit (no UI assets) |
| 2026-06-04 | `49c3431` | Harness TODO sweep complete |
| 2026-06-04 | `1dc58b2` | Session 2026-06-04 summary in CHANGELOG |
| 2026-06-04 | `f7643e8` | Archive PLAN Phases 1–7; Phase 8 harness status table |
| 2026-06-04 | `5dd8175` | Optional Prometheus scrape during `bench-large` (BENCHMARKS) |
| 2026-06-04 | `38ca7c6` | `fill-comparison-from-report.sh` for COMPARISON L1 publish |
| 2026-06-04 | `3b99883` | Final verify gate documentation @ `7a38d7f` |
| 2026-06-04 | `7a38d7f` | L2/L3 `render-report --dry-run` uses tier example JSON |
| 2026-06-04 | `161ee75` | Wire SLO gates into `run-large-benchmark-program.sh` after live G3/G4 |
| 2026-06-04 | `14f0980` | `check-tpuf-gates.sh` SLO script mirroring large-aws gates |
| 2026-06-04 | `d516003` | `check-large-aws-gates.sh` SLO script for large-aws JSON |
| 2026-06-04 | `8480ccb` | `render-report.sh` partial merge when one JSON side missing |
| 2026-06-04 | `7e1f876` | id-overlap live mode empty-namespace errors and preflight |
| 2026-06-04 | `387b7d0` | HANDOFF verify gate @ `af43e43` |
| 2026-06-04 | `af43e43` | `@spec` placeholders for live `large-aws-l1` / `tpuf-l1` JSON |
| 2026-06-04 | `fbddf10` | Nightly bench-100k ↔ large-dataset L1 program (G6) |
| 2026-06-04 | `a103ec0` | G2 MinIO `docker-compose.test.yml` docs in BENCHMARKS |
| 2026-06-04 | `4da0713` | Operator docs typo sweep |
| 2026-06-04 | `f30f46c` | HANDOFF: offline harness has no remaining gaps |
| 2026-06-04 | `8ca2eb4` | Sync bench-large/bench-tpuf `@spec` fact counts |
| 2026-06-04 | `e86a010` | CHANGELOG iterations 49–74 table |
| 2026-06-04 | `d09192d` | Live G3/G4 attempt 3 blocked (MinIO env, no `TURBOPUFFER_API_KEY`) |
| 2026-06-04 | `02398a8` | shellcheck: exit-codes and serve-ready lib fixes |
| 2026-06-04 | `a061bcd` | Standard exit codes for large-dataset benchmark scripts |
| 2026-06-04 | `f123909` | CI: pin Python 3.11 for benchmark workflows and verify gate |
| 2026-06-04 | `84bb89a` | [LARGE_DATASET_HARNESS_HANDOFF.md](../docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md) operator PR summary |
| 2026-06-04 | `f3103c8` | [workloads/QUERY_SPEC.md](workloads/QUERY_SPEC.md) for `queries.json` structure |
| 2026-06-04 | `04c50f1` | [workloads/EMBEDDINGS.md](workloads/EMBEDDINGS.md) — `bench_sin_v1` vs `xorshift_f32` |
| 2026-06-04 | `51a6f79` | Makefile `bench-verify` / `bench-dry-run` / `bench-g2-minio` |
| 2026-06-04 | `2b7984b` | JSON diff-friendly `.gitattributes` + `normalize-benchmark-json.sh` |
| 2026-06-04 | `756f1a8` | ISO8601 UTC timestamps on harness result JSON + validate Z-suffix gate |
| 2026-06-04 | `29f50fb` | `schema_version: large_benchmark_v1` on all harness result JSON |
| 2026-06-04 | `3c2ca6e` | Document ingest-large sequential batches (WAL + index lag) |
| 2026-06-04 | `57f7f39` | Index timeout troubleshooting + `diagnose-index-lag.sh` |
| 2026-06-04 | `ae22a7a` | Secret-echo audit gate and preflight hardening |
| 2026-06-04 | `1b5fb41` | Consolidate benchmark Python deps into `benchmarks/requirements.txt` |
| 2026-06-04 | `43fa706` | PLAN architecture mermaid with real scripts and operator sequence |
| 2026-06-04 | `f712750` | Fix harness audit changelog commit hash |
| 2026-06-04 | `9d5b87b` | Harness audit (operator handoff): verify exit 0 @ `9670556` |
| 2026-06-04 | `9670556` | Git policy for live results JSON (`check-benchmark-artifacts.sh` in verify gate) |
| 2026-06-04 | `4459e3a` | Skip live G4 when `TURBOPUFFER_API_KEY` unset |
| 2026-06-04 | `64654dd` | Large-benchmark cost estimator L1–L3 |
| 2026-06-04 | `fe6fc92` | Mandate ANN v3 for large-dataset program |
| 2026-06-04 | `622a1b8` | `GET /v1/ready` for S3-backed traffic readiness |
| 2026-06-04 | `cf5b490` | Add CHANGELOG_LARGE_DATASET harness commit history |
| 2026-06-04 | `aa7de00` | Shared serve readiness wait (`/health` or `/v1/ready`) before ingest/bench |
| 2026-06-04 | `5aca279` | G3 live retry skipped on MinIO; [OPERATOR_RUNBOOK_QUICK.md](OPERATOR_RUNBOOK_QUICK.md) |
| 2026-06-04 | `5627247` | Generalize benchmark JSON schemas for L2/L3 tiers + example artifacts |
| 2026-06-04 | `2337cbb` | `@spec` facts for validate-benchmark-json, preflight scripts, and schemas |
| 2026-06-04 | `3a3904a` | id-overlap JSON Schema, production-shaped mock, `id-overlap-l1.example.json` |
| 2026-06-04 | `7424cad` | COMPARISON operator checklist mirroring PLAN steps 1–5 |
| 2026-06-04 | `fe7edd3` | ingest-large-l1 JSON Schema wired into validate-benchmark-json |
| 2026-06-04 | `c91c063` | JSON Schema validation for large-aws and tpuf L1 artifacts |
| 2026-06-04 | `ff3eef8` | Extend shellcheck to report, overlap, MinIO gates, and render tests |
| 2026-06-04 | `5da0aa1` | tpuf driver ingest retry/resume mirroring ingest-large |
| 2026-06-04 | `868fd26` | shellcheck fixes across large-dataset benchmark harness scripts |
| 2026-06-04 | `4a7102a` | PLAN program status section (harness complete, live blocked) |
| 2026-06-04 | `d0bc8ba` | Harden ingest-large for production S3 retry and resume |
| 2026-06-04 | `23fe932` | Unified [benchmarks/README.md](README.md) hub for large-dataset program |
| 2026-06-04 | `58c3271` | GitHub Actions secrets doc for optional live large benchmarks |
| 2026-06-04 | `32cb61c` | Log G3/G4 live attempt skipped on MinIO env |
| 2026-06-04 | `bfaec74` | Add `verify-large-benchmark-program.sh` offline harness gate |
| 2026-06-04 | `a309b8d` | L2/L3 harness dry-run tier timeouts and operator docs |
| 2026-06-04 | `265c635` | G5: render-report measured mode (schema, interpretation, redaction) |
| 2026-06-04 | `fa08a69` | G4 turbopuffer operator runbook and `preflight-tpuf.sh` |
| 2026-06-04 | `ee30fd1` | G3 EC2+S3 operator runbook and `preflight-aws-ec2.sh` |
| 2026-06-04 | `33a14d1` | Wire MinIO L1 schema 10k fast path into `g2-minio-correctness` CI |
| 2026-06-04 | `476a753` | MinIO schema `--docs 10000` fast path; fix v3 cold probe integration gate |
| 2026-06-04 | `433d2fd` | Nightly large-dataset G2 + program dry-run (G6) |
| 2026-06-04 | `833e1bc` | Regenerate MinIO L1 schema example with full bench JSON |
| 2026-06-04 | `250257b` | `run-large-benchmark-program.sh` end-to-end operator chain |
| 2026-06-04 | `f877d64` | bench-large: record filter/hybrid query runs in large-aws JSON |
| 2026-06-04 | `98a9ff7` | Ingest timing breakdown in large-tier JSON (plan §4.1) |
| 2026-06-04 | `696a247` | tpuf filter/hybrid per-query latency and optional warm phase |
| 2026-06-04 | `5ccf9eb` | bench-large optional `--warm` protocol (plan §4.3) |
| 2026-06-04 | `2cccc7e` | G2: run all filter and hybrid queries from queries.json |
| 2026-06-04 | `3ddf464` | PLAN audit checklist, unresolved assumptions, harness map |
| 2026-06-04 | `bd449b6` | G5 exemplar + MinIO L1 schema example JSON (`environment=minio`) |
| 2026-06-04 | `95197a9` | G4 `run-tpuf-large-benchmark` operator harness |
| 2026-06-04 | `a1b34cc` | G3 AWS large-benchmark wrapper and shared preflight |
| 2026-06-04 | `ef4fa97` | PLAN: mark MinIO integration+bench green on verification checklist |
| 2026-06-04 | `52e3208` | Phase 3.3 cross-engine id overlap spot-check |
| 2026-06-04 | `67c7050` | CI: wire G2 MinIO correctness gates; fix synthetic_128 cold metric |
| 2026-06-04 | `1902c62` | CI: `benchmark-large-dispatch` workflow (A6 dry-run gates) |
| 2026-06-04 | `76ff071` | Commit synthetic-128 L2/L3 manifests and tier dry-run gates |
| 2026-06-04 | `8594099` | BENCHMARKS.md Phase 4–6 large-dataset operator runbook |
| 2026-06-04 | `77a45bb` | `@spec` bench-large and bench-tpuf facts for comparison harness |
| 2026-06-04 | `5972ab7` | Wire synthetic-128 G2 correctness gates (Phase 2) |
| 2026-06-04 | `59f5822` | A5: `render-report.sh` to merge large-aws and tpuf JSON |
| 2026-06-04 | `08e66ce` | A4: turbopuffer driver for large-tier comparison |
| 2026-06-04 | `9272b8f` | A3: `bench-large.sh` for tiered AWS cold benchmarks |
| 2026-06-04 | `d788b65` | A2: `ingest-large.sh` for synthetic workload ingest |
| 2026-06-04 | `6186190` | A1: synthetic-128 workload generator (Phase 1) |

---

## Related docs

- [PLAN_LARGE_DATASET_BENCHMARK.md](../docs/PLAN_LARGE_DATASET_BENCHMARK.md) — goals G1–G6, phase matrix, verification checklist
- [LARGE_DATASET_HARNESS_HANDOFF.md](../docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md) — operator PR summary (iter 71)
- [README.md](README.md) — directory layout, JSON commit policy, script index
- [OPERATOR_RUNBOOK_QUICK.md](OPERATOR_RUNBOOK_QUICK.md) — EC2 live run one-pager
- [OPERATOR_AFTER_LIVE_RUN.md](OPERATOR_AFTER_LIVE_RUN.md) — post-live JSON validation and `@spec` activation (iter 108)
- [workloads/QUERY_SPEC.md](workloads/QUERY_SPEC.md) — `queries.json` contract (iter 65)
- [workloads/EMBEDDINGS.md](workloads/EMBEDDINGS.md) — vector generation semantics (iter 70)