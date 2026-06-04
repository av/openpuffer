# Large-dataset benchmark harness — changelog

Chronological record of commits that built the **offline harness** for [PLAN_LARGE_DATASET_BENCHMARK.md](../docs/PLAN_LARGE_DATASET_BENCHMARK.md) (A1–A6, G2–G6, JSON schemas, operator wrappers). **Live G3/G4 JSON and measured G5** are still operator-pending; see [results/OPERATOR_G3_G4_ATTEMPT.md](results/OPERATOR_G3_G4_ATTEMPT.md).

**Verify gate:** `./scripts/verify-large-benchmark-program.sh` (exit **0** @ `7a38d7f`, 2026-06-04T02:54:51Z)

**Session log:** timeboxed run 2026-06-04 (~90 iterations); progress file `/tmp/timeboxed-large-dataset-benchmark-plan-1780525473.md`

---

## Session 2026-06-04

| Field | Value |
|-------|--------|
| **Time window** | 2026-06-04 00:24 CEST → ~06:00 CEST (timeboxed PLAN_LARGE_DATASET_BENCHMARK implementation) |
| **Total iterations** | **~90** (A1–A6 + G2–G6 harness build, polish, closure; see tables below) |
| **Harness commits** | `6186190` … `f7643e8` (offline program complete) |
| **Verify gate** | `./scripts/verify-large-benchmark-program.sh` exit **0** @ **`7a38d7f`** (2026-06-04T02:54:51Z; optional `--with-g2` for MinIO Docker parity) |

### Harness complete

All **offline** checklist rows for [PLAN_LARGE_DATASET_BENCHMARK.md](../docs/PLAN_LARGE_DATASET_BENCHMARK.md) are satisfied: synthetic workload (A1), ingest/bench/tpuf drivers (A2–A4), report merge + measured renderer (A5), CI dispatch + nightly dry-run (A6), G2 MinIO correctness gates, G3/G4 operator wrappers + preflights, G5 schema/exemplar JSON + `fill-comparison-from-report.sh`, G6 regression wiring, JSON schemas L1–L3, id-overlap mock, SLO gate scripts (`check-large-aws-gates`, `check-tpuf-gates`), and the unified verify gate.

**Statement:** The large-dataset benchmark **harness is complete** and safe for operator handoff / merge. `./scripts/verify-large-benchmark-program.sh` is the single offline gate; last full audit passed at `7a38d7f`.

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

**Operator sequence (L1):** preflight AWS → `run-aws-large-benchmark.sh` → preflight tpuf → `run-tpuf-large-benchmark.sh` → `run-id-overlap-spotcheck.sh` → `run-large-benchmark-program.sh --measured-report` (or `render-report.sh` + `fill-comparison-from-report.sh`).

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

---

## All commits (newest first)

| Date | Commit | Description |
|------|--------|-------------|
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
- [workloads/QUERY_SPEC.md](workloads/QUERY_SPEC.md) — `queries.json` contract (iter 65)
- [workloads/EMBEDDINGS.md](workloads/EMBEDDINGS.md) — vector generation semantics (iter 70)