# Benchmarks hub

Large-dataset **openpuffer vs turbopuffer** comparison assets, workloads, and result JSON. Operator procedures and SLO tables live in [docs/BENCHMARKS.md](../docs/BENCHMARKS.md); program goals and phase checklist in [docs/PLAN_LARGE_DATASET_BENCHMARK.md](../docs/PLAN_LARGE_DATASET_BENCHMARK.md).

## Directory layout

```
benchmarks/
├── README.md                 ← this hub
├── workloads/
│   ├── generate_synthetic.py # deterministic manifest + queries + spot_check
│   ├── test_generate_synthetic.py
│   └── synthetic-128/        # committed L1/L2/L3 tiers (seed 42, dim 128)
│       ├── l1-100k/          manifest.json, queries.json
│       ├── l2-500k/
│       └── l3-1m/
├── tpuf_driver/              # Python G4 driver (same workload as ingest/bench)
│   ├── run_benchmark.py
│   ├── test_run_benchmark.py
│   └── requirements.txt
├── cross_check/              # Phase 3.3 id overlap spot-check
│   ├── id_overlap.py
│   ├── run_spotcheck.py
│   └── fixtures/
├── report/
│   ├── README.md
│   └── fixtures/             # offline dry-run inputs for render-report
└── results/                  # bench JSON artifacts (see table below)
```

**Related (outside `benchmarks/`):**

| Path | Role |
|------|------|
| `scripts/` | Shell orchestration (ingest, bench, G3/G4, verify, report) |
| `scripts/lib/large-benchmark-preflight.sh` | Shared S3/tpuf/workload guards |
| `tests/common/synthetic_workload.rs` | Rust loader for G2 integration/bench gates |
| `docs/reports/` | Generated `BENCHMARK_VS_TURBOPUFFER_<date>.md` |
| `.facts` | `@spec` facts tagged `bench-large`, `bench-tpuf` |

Subdirectory docs: [workloads/synthetic-128/README.md](workloads/synthetic-128/README.md), [tpuf_driver/README.md](tpuf_driver/README.md), [cross_check/README.md](cross_check/README.md), [report/README.md](report/README.md).

## JSON artifacts — committed vs operator-only

There is **no** `benchmarks/` entry in `.gitignore`. What lands in git is a **policy**, enforced by preflight (`large_preflight_guard_aws_results_path`, `preflight-tpuf.sh --check-results`) and review.

### Committed in the repo

| Path | Purpose |
|------|---------|
| `workloads/synthetic-128/**/manifest.json` | Tier doc counts, seed, schema |
| `workloads/synthetic-128/**/queries.json` | Cold/filter/hybrid/warm/spot_check/recall_defaults |
| `report/fixtures/*.json` | Offline G5 merge inputs (`--dry-run`) |
| `cross_check/fixtures/*.json` | Mock overlap for tests |
| `results/baseline-10k.json` | Legacy 10k MinIO baseline |
| `results/cold-50k-v3.json` | Legacy 50k v3 cold gate snapshot |
| `results/nightly-100k.json` | Nightly 100k MinIO snapshot |
| `results/*-schema-minio*.example.json` | MinIO **shape** exemplars (`environment=minio`) |
| `results/ingest-large-*-schema-minio*.example.json` | Ingest sidecars for schema examples |

### Operator-produced — commit after live AWS/tpuf runs

| Path | When to commit |
|------|----------------|
| `results/large-aws-{l1,l2,l3}.json` | G3 on **real AWS S3** (`environment` ≠ `minio`); run `preflight-aws-ec2.sh` / preflight guard |
| `results/tpuf-{l1,l2,l3}.json` | G4 live driver; run `preflight-tpuf.sh --check-results` before `git add` |
| `results/ingest-large-{tier}.json` | Optional sidecar from `ingest-large.sh` (embedded in bench JSON too) |
| `results/id-overlap-{tier}.json` | Phase 3.3 after **both** namespaces indexed |

### Do not commit (or use `*.example.json` naming)

| Pattern | Reason |
|---------|--------|
| `large-aws-*.json` from **MinIO** | Not comparable to tpuf; preflight blocks unless path contains `minio`, `example`, or `schema` |
| Scratch / partial runs | No `environment`, wrong tier, or failed index gate |
| Files containing API keys | `preflight-tpuf.sh --check-results` scans before publish |

**MinIO schema runs** must keep `environment=minio` and filenames like `large-aws-l1-schema-minio.example.json` — safe for CI and schema facts; **never** paste MinIO latencies into [docs/COMPARISON.md](../docs/COMPARISON.md).

## Operator quick-start

Three steps: **verify locally** → **dry-run the program** → **live on EC2** (AWS + tpuf creds).

### 1. Verify harness (no cloud spend)

```bash
./scripts/verify-large-benchmark-program.sh
# optional: --with-g2 (MinIO Docker G2), --skip-l2-l3 (faster L1-only)
```

Runs pytest (workloads, tpuf driver, id-overlap), render-report tests, ingest/bench schema tests, `synthetic_workload_gate`, and L1–L3 harness dry-runs. Same gate as CI [benchmark-large-dispatch.yml](../.github/workflows/benchmark-large-dispatch.yml).

### 2. Program dry-run (no credentials)

```bash
./scripts/run-large-benchmark-program.sh --dry-run --tier l1
```

Prints the full G2→G3→G4→overlap→report plan and writes fixture-based report skeleton. Per-step dry-runs: `ingest-large.sh`, `bench-large.sh`, `run-aws-large-benchmark.sh`, `run-tpuf-large-benchmark.sh`, `run-id-overlap-spotcheck.sh`, `render-report.sh --dry-run`.

### 3. Live comparison on EC2

On an **m7i.xlarge** (or similar) in the **same region/AZ** as the S3 bucket:

```bash
export OPENPUFFER_S3_BUCKET=openpuffer-bench-<account>-us-east-1
export OPENPUFFER_S3_REGION=us-east-1
export OPENPUFFER_ANN_VERSION=3
./scripts/preflight-aws-ec2.sh

export TURBOPUFFER_API_KEY=tpuf_...
export TURBOPUFFER_REGION=aws-us-east-1
export TURBOPUFFER_BENCH_DELETE_FIRST=1
./scripts/preflight-tpuf.sh --tier l1

./scripts/run-large-benchmark-program.sh --tier l1
# or stepwise: run-aws-large-benchmark.sh → run-tpuf-large-benchmark.sh → run-id-overlap-spotcheck.sh

./scripts/preflight-tpuf.sh --check-results benchmarks/results/tpuf-l1.json
./scripts/run-large-benchmark-program.sh --tier l1 --measured-report
```

GitHub Actions alternative (secrets): [docs/BENCHMARKS_GITHUB_ACTIONS_SECRETS.md](../docs/BENCHMARKS_GITHUB_ACTIONS_SECRETS.md) + [benchmark-large-live.yml](../.github/workflows/benchmark-large-live.yml) (`enable_live_run` default **false**).

## Scripts (one line each)

### Orchestration

| Script | Description |
|--------|-------------|
| [verify-large-benchmark-program.sh](../scripts/verify-large-benchmark-program.sh) | Offline gate: pytest + dry-runs + optional G2 + facts |
| [run-large-benchmark-program.sh](../scripts/run-large-benchmark-program.sh) | Chain G2→G3→G4→overlap→G5; `--dry-run`, `--measured-report`, `--warm` |
| [run-aws-large-benchmark.sh](../scripts/run-aws-large-benchmark.sh) | G3: G2 subset → AWS S3 → ingest-large → bench-large → `large-aws-{tier}.json` |
| [run-tpuf-large-benchmark.sh](../scripts/run-tpuf-large-benchmark.sh) | G4: optional G2 → tpuf preflight → `run_benchmark.py` → `tpuf-{tier}.json` |
| [run-minio-correctness-gates.sh](../scripts/run-minio-correctness-gates.sh) | G2 MinIO correctness subset (fixture + integration smoke) |
| [run-minio-large-schema-example.sh](../scripts/run-minio-large-schema-example.sh) | MinIO JSON **schema** only (`*.example.json`, not COMPARISON) |
| [run-id-overlap-spotcheck.sh](../scripts/run-id-overlap-spotcheck.sh) | Phase 3.3 wrapper → `id-overlap-{tier}.json` |

### Core harness (A2–A5)

| Script | Description |
|--------|-------------|
| [ingest-large.sh](../scripts/ingest-large.sh) | Generator-driven upsert batches (10k/batch) + retry/resume + index poll → `preferred_ann_version == 3` |
| [bench-large.sh](../scripts/bench-large.sh) | Cold/filter/hybrid/warm queries → `large-aws-{tier}.json` |
| [render-report.sh](../scripts/render-report.sh) | Merge openpuffer + tpuf JSON → `docs/reports/BENCHMARK_VS_TURBOPUFFER_<date>.md` |

### Preflight

| Script | Description |
|--------|-------------|
| [preflight-aws-ec2.sh](../scripts/preflight-aws-ec2.sh) | EC2 IMDS, region/AZ, instance profile, S3 `head-bucket` before G3 |
| [preflight-tpuf.sh](../scripts/preflight-tpuf.sh) | API key, region vs AWS, RTT, cost estimate, results secret scan |
| [lib/large-benchmark-preflight.sh](../scripts/lib/large-benchmark-preflight.sh) | Shared tier/workload/MinIO→AWS path guards |

### Tests & CI helpers

| Script | Description |
|--------|-------------|
| [test_l2-l3-harness-dry-run.sh](../scripts/test_l2-l3-harness-dry-run.sh) | One-shot L2+L3 offline harness validation |
| [test_render-report.sh](../scripts/test_render-report.sh) | Offline render-report merge checks |
| [test_render-report-measured.sh](../scripts/test_render-report-measured.sh) | Measured-mode schema, interpretation, appendix redaction |
| [test_ingest-timing-schema.sh](../scripts/test_ingest-timing-schema.sh) | `ingest_timing` / batch_runs JSON shape |
| [test_bench-large-secondary-schema.sh](../scripts/test_bench-large-secondary-schema.sh) | Filter/hybrid/warm fields in bench JSON |
| [ensure-compose-minio.sh](../scripts/ensure-compose-minio.sh) | Start/wait for compose MinIO on `:9000` (CI schema job) |

### Legacy / related

| Script | Description |
|--------|-------------|
| [bench-1m.sh](../scripts/bench-1m.sh) | Legacy 1M bench path; prefer `bench-large.sh --tier l3` for shared workload |
| [run-integration-s3.sh](../scripts/run-integration-s3.sh) | Full MinIO integration tests (includes G2 `synthetic_128_*` gates) |

### In-tree Python

| Entry | Description |
|-------|-------------|
| [workloads/generate_synthetic.py](workloads/generate_synthetic.py) | Emit manifest, queries, vectors for a tier |
| [tpuf_driver/run_benchmark.py](tpuf_driver/run_benchmark.py) | turbopuffer ingest + cold/filter/hybrid/warm + recall |
| [cross_check/run_spotcheck.py](cross_check/run_spotcheck.py) | Top-k id overlap between engines (`--dry-run`, `--mock`, live) |

## Facts & CI

```bash
facts check --tags bench-large
facts check --tags bench-tpuf
```

- **Dispatch (dry-run):** [.github/workflows/benchmark-large-dispatch.yml](../.github/workflows/benchmark-large-dispatch.yml)
- **Nightly program smoke:** `large-dataset-program` in [.github/workflows/nightly-stress.yml](../.github/workflows/nightly-stress.yml)
- **G2 + 10k schema:** `g2-minio-correctness` in [.github/workflows/ci.yml](../.github/workflows/ci.yml)

## Further reading

- [docs/BENCHMARKS.md](../docs/BENCHMARKS.md) — SLOs, G2/G3/G4 runbooks, L2/L3 expectations
- [docs/COMPARISON.md](../docs/COMPARISON.md) — product comparison + measured L1 table (after live JSON)
- [docs/reports/BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md](../docs/reports/BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md) — report layout (`NOT MEASURED`)