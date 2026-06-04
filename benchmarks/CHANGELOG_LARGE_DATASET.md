# Large-dataset benchmark harness — changelog

Chronological record of commits that built the **offline harness** for [PLAN_LARGE_DATASET_BENCHMARK.md](../docs/PLAN_LARGE_DATASET_BENCHMARK.md) (A1–A6, G2–G6, JSON schemas, operator wrappers). **Live G3/G4 JSON and measured G5** are still operator-pending; see [results/OPERATOR_G3_G4_ATTEMPT.md](results/OPERATOR_G3_G4_ATTEMPT.md).

**Verify gate:** `./scripts/verify-large-benchmark-program.sh`

---

## By program track

| Track | Commits | One-line summary |
|-------|---------|------------------|
| **A1** workload | `6186190`, `76ff071` | Deterministic `generate_synthetic.py` + committed L1/L2/L3 manifests and queries |
| **A2** ingest | `d788b65`, `98a9ff7`, `d0bc8ba`, `5da0aa1`, `aa7de00` | `ingest-large.sh` with timing JSON, S3 retry/resume, serve readiness poll, tpuf ingest parity |
| **A3** bench | `9272b8f`, `f877d64`, `5ccf9eb` | `bench-large.sh` tiered cold/filter/hybrid/warm → `large-aws-{tier}.json` |
| **A4** tpuf driver | `08e66ce`, `696a247` | `run_benchmark.py` ingest + cold/filter/hybrid/warm + recall on shared workload |
| **A5** report | `59f5822`, `265c635`, `bd449b6` | `render-report.sh` merge; measured-mode schema/interpretation/redaction; exemplar + MinIO shape JSON |
| **A6** CI dispatch | `1902c62`, `58c3271` | `benchmark-large-dispatch.yml` dry-run; optional live workflow secrets doc |
| **G2** correctness | `5972ab7`, `67c7050`, `ef4fa97`, `2cccc7e`, `33a14d1`, `476a753` | MinIO integration/bench gates, full filter/hybrid query coverage, CI `g2-minio-correctness`, 10k schema fast path |
| **G3** AWS operator | `a1b34cc`, `ee30fd1`, `5aca279` | `run-aws-large-benchmark.sh`, `preflight-aws-ec2.sh`, EC2 quick runbook (live blocked on MinIO dev host) |
| **G4** tpuf operator | `95197a9`, `fa08a69` | `run-tpuf-large-benchmark.sh`, `preflight-tpuf.sh`, G4 operator runbook |
| **Phase 3.3** overlap | `52e3208`, `3a3904a` | Cross-engine id spot-check + JSON Schema + production-shaped mock |
| **E2E** orchestration | `250257b`, `bfaec74`, `a309b8d` | `run-large-benchmark-program.sh` chain; `verify-large-benchmark-program.sh`; L2/L3 dry-run timeouts |
| **JSON schemas** | `c91c063`, `fe7edd3`, `5627247` | `validate-benchmark-json.sh` for large-aws, tpuf, ingest, id-overlap (L1–L3 generalized) |
| **G6** regression | `433d2fd`, `833e1bc` | Nightly `large-dataset-program` dry-run; full MinIO L1 schema example regeneration |
| **@spec** facts | `77a45bb`, `2337cbb` | `bench-large` / `bench-tpuf` tags; validate, preflight, schema facts |
| **Docs / hub** | `8594099`, `23fe932`, `3ddf464`, `4a7102a`, `7424cad`, `32cb61c` | BENCHMARKS phases 4–6 runbook; benchmarks README; PLAN status; COMPARISON checklist; G3/G4 attempt log |
| **Harness hygiene** | `868fd26`, `ff3eef8` | shellcheck coverage for benchmark shell scripts and tests |

---

## All commits (newest first)

| Date | Commit | Description |
|------|--------|-------------|
| 2026-06-04 | `aa7de00` | Shared serve readiness wait (`/health` or `/v1/ready`) before ingest/bench upsert/query |
| 2026-06-04 | `5aca279` | G3 live retry skipped on MinIO; add [OPERATOR_RUNBOOK_QUICK.md](OPERATOR_RUNBOOK_QUICK.md) |
| 2026-06-04 | `5627247` | Generalize benchmark JSON schemas for L2/L3 tiers + example artifacts |
| 2026-06-04 | `2337cbb` | `@spec` facts for validate-benchmark-json, preflight scripts, and schemas |
| 2026-06-04 | `3a3904a` | id-overlap JSON Schema, production-shaped mock, committed `id-overlap-l1.example.json` |
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
- [README.md](README.md) — directory layout, JSON commit policy, script index
- [OPERATOR_RUNBOOK_QUICK.md](OPERATOR_RUNBOOK_QUICK.md) — EC2 live run one-pager