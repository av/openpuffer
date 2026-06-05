# Benchmarks hub

Large-dataset **openpuffer vs turbopuffer** comparison assets, workloads, and result JSON. Operator procedures and SLO tables live in [docs/BENCHMARKS.md](../docs/BENCHMARKS.md); program goals and phase checklist in [docs/PLAN_LARGE_DATASET_BENCHMARK.md](../docs/PLAN_LARGE_DATASET_BENCHMARK.md). **Operator handoff (PR summary):** [docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md](../docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md). **Harness commit history:** [CHANGELOG_LARGE_DATASET.md](CHANGELOG_LARGE_DATASET.md). **EC2 live run (one page):** [OPERATOR_RUNBOOK_QUICK.md](OPERATOR_RUNBOOK_QUICK.md). **After EC2 (activate live `@spec` facts `8zb` / `7ow`):** [OPERATOR_AFTER_LIVE_RUN.md](OPERATOR_AFTER_LIVE_RUN.md).

## Directory layout

```
benchmarks/
├── README.md                 ← this hub
├── specs/tpuf/               # vendored tpuf-benchmark website TOML (10M × 1024)
├── requirements.txt          # consolidated Python deps (all harness modules)
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
│   └── requirements.txt      # compat shim → ../requirements.txt
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
| `.facts` | `@spec` facts tagged `bench-large`, `bench-tpuf`, `bench-op-scaling` |

Subdirectory docs: [workloads/synthetic-128/README.md](workloads/synthetic-128/README.md), [workloads/QUERY_SPEC.md](workloads/QUERY_SPEC.md), [tpuf_driver/README.md](tpuf_driver/README.md), [cross_check/README.md](cross_check/README.md), [report/README.md](report/README.md).

## Python dependencies

All in-tree benchmark Python (workloads tests, tpuf driver, id-overlap, JSON Schema validation) shares one lockfile:

| Package | Used by |
|---------|---------|
| `pytest` | `workloads/`, `tpuf_driver/`, `cross_check/` tests |
| `jsonschema` | [`scripts/validate-benchmark-json.sh`](../scripts/validate-benchmark-json.sh) |
| `turbopuffer` | G4 driver + live id-overlap |
| `httpx` | tpuf driver offline tests |

**Python 3.11+** (checked by `ensure_benchmark_python_version` in [`scripts/lib/benchmark-python-deps.sh`](../scripts/lib/benchmark-python-deps.sh)). CI benchmark workflows pin **3.11** via `actions/setup-python`.

```bash
./scripts/install-benchmark-python-deps.sh
# or: pip install -r benchmarks/requirements.txt
```

[`scripts/verify-large-benchmark-program.sh`](../scripts/verify-large-benchmark-program.sh) and CI workflows call the install script before pytest and schema gates. Legacy path `benchmarks/tpuf_driver/requirements.txt` is a shim to `benchmarks/requirements.txt`.

## JSON artifacts — committed vs operator-only

Git policy is enforced at **write time** (preflight guards), **add time** (`.gitignore` + explicit `git add -f`), and **CI/review** ([`scripts/check-benchmark-artifacts.sh`](../scripts/check-benchmark-artifacts.sh)).

### Committed in the repo (always tracked)

| Path | Purpose |
|------|---------|
| `workloads/synthetic-128/**/manifest.json` | Tier doc counts, seed, schema |
| `workloads/synthetic-128/**/queries.json` | Cold/filter/hybrid/warm/spot_check/recall_defaults |
| `report/fixtures/*.json` | Offline G5 merge inputs (`--dry-run`; `environment=aws-s3` / `turbopuffer:<region>`) |
| `cross_check/fixtures/*.json` | Mock overlap for tests |
| `results/baseline-10k.json` | Legacy 10k MinIO baseline (`environment=minio-testcontainers`) |
| `results/cold-50k-v3.json` | Legacy 50k v3 cold gate snapshot |
| `results/nightly-100k.json` | Nightly 100k MinIO snapshot |
| `results/tpuf-official-reference.json` | Official tpuf 10M×1024 homepage latencies + spec pointers (scaling comparison) |
| `results/op-scaling-{10k,50k,100k}.json`, `op-scaling-{10k,100k}-warm.json`, `op-scaling-10k-synthetic128.json` | MinIO cold/warm scaling tiers (release+v3); [`scripts/run-op-scaling-benchmark.sh`](../scripts/run-op-scaling-benchmark.sh) |
| `specs/tpuf/vector-10m-{cold,hot}.toml` | Vendored from [tpuf-benchmark/website](https://github.com/turbopuffer/tpuf-benchmark/tree/main/benchmarks/website) |
| `results/*-schema-minio*.example.json` | MinIO **shape** exemplars (`environment=minio`) |
| `results/ingest-large-*-schema-minio*.example.json` | Ingest sidecars for schema examples |
| `results/large-aws-l{2,3}.example.json`, `tpuf-l{2,3}.example.json`, `ingest-large-l{2,3}.example.json` | L2/L3 schema placeholders (`environment=aws-s3` or `turbopuffer:*`) |
| `results/id-overlap-l1.example.json` | Phase 3.3 mock overlap (no live API) |

### Operator-produced — commit after live AWS/tpuf runs

These paths are in [`.gitignore`](../.gitignore) so a normal `git add benchmarks/results/` does **not** pick up local runs. After EC2 preflight and validation, commit **explicitly**:

```bash
./scripts/validate-benchmark-json.sh benchmarks/results/large-aws-l1.json
./scripts/preflight-tpuf.sh --check-results benchmarks/results/tpuf-l1.json
./scripts/check-benchmark-artifacts.sh --staged
git add -f benchmarks/results/large-aws-l1.json benchmarks/results/ingest-large-l1.json
git add -f benchmarks/results/tpuf-l1.json benchmarks/results/id-overlap-l1.json
```

| Path | When to commit | Required `environment` |
|------|----------------|------------------------|
| `results/large-aws-{l1,l2,l3}.json` | G3 on **real AWS S3** | `aws-s3` |
| `results/ingest-large-{tier}.json` | Optional ingest sidecar | `aws-s3` |
| `results/tpuf-{l1,l2,l3}.json` | G4 live driver | `turbopuffer:<region>` |
| `results/id-overlap-{tier}.json` | Phase 3.3 after both sides indexed | (no `environment` field) |

### Do not commit (or use `*.example.json` / `*-schema-minio*` naming)

| Pattern | Reason |
|---------|--------|
| `large-aws-l*.json` from **MinIO** | Not comparable to tpuf; use `large-aws-l1-schema-minio.example.json` (`environment=minio`) |
| Scratch / partial runs | No `environment`, wrong tier, or failed index gate |
| Files containing API keys | `preflight-tpuf.sh --check-results` scans before publish |

**Why `.gitignore` live paths?** Prevents accidental commits of workstation/MinIO timings while still allowing measured AWS/tpuf JSON in the repo via `git add -f` after operator review. **`check-benchmark-artifacts.sh`** fails CI if tracked JSON has `environment=minio` on a live comparison basename.

**MinIO schema runs** must keep `environment=minio` and filenames like `large-aws-l1-schema-minio.example.json` — safe for CI and schema facts; **never** paste MinIO latencies into [docs/COMPARISON.md](../docs/COMPARISON.md).

## JSON formatting & git diff

Committed benchmark JSON should be **canonical pretty-print** so `git diff` shows semantic changes only (not whitespace, key order drift, or missing final newlines).

| Mechanism | Role |
|-----------|------|
| [`.gitattributes`](../.gitattributes) | `text eol=lf` on all `benchmarks/**/*.json`; `linguist-generated` + `merge=union` on regenerated workload `queries.json` / `manifest.json`; `merge=union` on fixtures and `*.example.json` for parallel tier edits; schemas use normal merge |
| [`scripts/normalize-benchmark-json.sh`](../scripts/normalize-benchmark-json.sh) | Rewrite (or `--check`) with **jq** (`--indent 2 --ascii-output`) except **`queries.json`**, which uses **Python** `json.dumps` to match [`generate_synthetic.py`](workloads/generate_synthetic.py) float formatting |

Before committing new or edited benchmark JSON:

```bash
./scripts/normalize-benchmark-json.sh benchmarks/report/fixtures/large-aws-l1.json
# or normalize the default tracked set:
./scripts/normalize-benchmark-json.sh

./scripts/normalize-benchmark-json.sh --check   # CI gate (subset in test_normalize-benchmark-json.sh)
```

After regenerating workloads:

```bash
python3 benchmarks/workloads/generate_synthetic.py --tier l1
./scripts/normalize-benchmark-json.sh benchmarks/workloads/synthetic-128/l1-100k/
```

**Do not** run jq directly on `queries.json` — jq reformats floats (e.g. `5.56e-05` vs `0.0000556…`) and produces noisy diffs. Use the normalize script or the generator only.

## Operator quick-start

Three steps: **verify locally** → **dry-run the program** → **live on EC2** (AWS + tpuf creds).

### 0. Python deps (once per host)

```bash
./scripts/install-benchmark-python-deps.sh
```

### 1. Verify harness (no cloud spend)

```bash
make bench-verify
# same as:
./scripts/verify-large-benchmark-program.sh
# optional: make bench-verify VERIFY_FLAGS="--skip-l2-l3"  # faster L1-only
# optional: make bench-g2-minio   # MinIO G2 only (Docker; not part of bench-verify)
```

Runs pytest (workloads, tpuf driver, id-overlap), render-report tests, ingest/bench schema tests, `synthetic_workload_gate`, L1–L3 harness dry-runs, and **op vs tpuf scaling smoke** on committed `op-scaling-*.json` (no MinIO bench in verify). CI [`ci.yml`](../.github/workflows/ci.yml) job `op-scaling-comparison` runs the same fast gate; [benchmark-large-dispatch.yml](../.github/workflows/benchmark-large-dispatch.yml) runs full `verify-large-benchmark-program.sh`.

#### openpuffer vs turbopuffer scaling (committed JSON)

**CI / offline smoke** (no Docker; uses committed results):

```bash
./scripts/verify-op-scaling-comparison.sh
# or: make bench-verify-op-scaling
```

**Refresh full MinIO scaling suite** (slow — 10k/50k/100k/warm + synthetic-128; **not** run in CI):

```bash
make bench-op-scaling          # regenerate benchmarks/results/op-scaling-*.json
make bench-compare-tpuf        # print extrapolation vs tpuf-official-reference.json
./scripts/validate-benchmark-json.sh benchmarks/results/op-scaling-*.json
git add benchmarks/results/op-scaling-*.json
```

Report: [docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md](../docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md) (charts + methodology); iteration log: [OPENS_VS_TPUF_SCALING_COMPARISON.md](../docs/reports/OPENS_VS_TPUF_SCALING_COMPARISON.md). **5-command quickstart:** [SCALING_VS_TPUF_QUICKSTART.md](SCALING_VS_TPUF_QUICKSTART.md). Operator verdict: `./scripts/print-scaling-verdict.sh`. Skip scaling inside full verify: `VERIFY_FLAGS="--skip-op-scaling"`.

### 2. Program dry-run (no credentials)

```bash
make bench-dry-run
# or a single tier plan:
./scripts/run-large-benchmark-program.sh --dry-run --tier l1
```

`make bench-dry-run` runs L1 per-script dry-runs plus L2/L3 via [`test_l2-l3-harness-dry-run.sh`](../scripts/test_l2-l3-harness-dry-run.sh). No pytest, cargo tests, or facts — faster than `bench-verify`.

Per-step dry-runs: `./scripts/ingest-large.sh`, `./scripts/bench-large.sh`, `./scripts/run-aws-large-benchmark.sh`, `./scripts/run-tpuf-large-benchmark.sh`, `./scripts/run-id-overlap-spotcheck.sh`, `./scripts/render-report.sh --dry-run`.

### 2b. Operator preflight (G3+G4+overlap)

```bash
make bench-preflight
# same as:
./scripts/preflight-large-benchmark-all.sh
# optional: make bench-preflight PREFLIGHT_FLAGS="--skip-overlap"   # aws+tpuf only (no openpuffer yet)
# EC2 live: make bench-preflight PREFLIGHT_FLAGS="--live --tier l1"
```

Default mode runs AWS cost estimate (`preflight-aws-ec2.sh --dry-run`), turbopuffer workload/deps (`preflight-tpuf.sh --skip-key`), and id-overlap checks (`preflight-id-overlap.sh --skip-key`). Live mode matches `run-*-large-benchmark.sh --preflight-only` spend gates. See [OPERATOR_RUNBOOK_QUICK.md](OPERATOR_RUNBOOK_QUICK.md).

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
# or stepwise: ./scripts/run-aws-large-benchmark.sh → ./scripts/run-tpuf-large-benchmark.sh → ./scripts/run-id-overlap-spotcheck.sh

./scripts/preflight-tpuf.sh --check-results benchmarks/results/tpuf-l1.json
./scripts/run-large-benchmark-program.sh --tier l1 --measured-report
```

GitHub Actions alternative (secrets): [docs/BENCHMARKS_GITHUB_ACTIONS_SECRETS.md](../docs/BENCHMARKS_GITHUB_ACTIONS_SECRETS.md) + [benchmark-large-live.yml](../.github/workflows/benchmark-large-live.yml) (`enable_live_run` default **false**).

## Exit codes

Large-dataset **operator** shell scripts share constants in [`scripts/lib/large-benchmark-exit-codes.sh`](../scripts/lib/large-benchmark-exit-codes.sh). Use `echo $?` after a failed step to branch automation (retry preflight vs fix serve vs wait longer for index).

| Code | Name | When |
|------|------|------|
| **0** | OK | Dry-run plan printed, preflight passed, ingest/bench completed, verify gate green |
| **1** | Preflight | Missing/invalid env (S3 bucket, keys, tpuf API key), region/AZ mismatch, workload manifest missing, MinIO endpoint on G3 AWS path, G2 subset failed, artifact policy/secret scan |
| **2** | Serve timeout | `ingest-large` / `bench-large` could not get HTTP 2xx from `GET /v1/ready` or `GET /health` within `OPENPUFFER_SERVE_READY_TIMEOUT_SEC` (default 120s) |
| **3** | Index timeout | Ingest (or bench re-index wait) timed out waiting for `index_cursor == wal_commit_seq` and `preferred_ann_version == 3`; see [`diagnose-index-lag.sh`](../scripts/diagnose-index-lag.sh) |
| **64** | Usage | Unknown CLI flag on `preflight-aws-ec2.sh` / `preflight-tpuf.sh` only |

**Scripts that emit the table above**

| Script | Typical non-zero codes |
|--------|-------------------------|
| `preflight-aws-ec2.sh` | 1 (S3/region/creds), 64 (usage) |
| `preflight-tpuf.sh` | 1 (key/region/workload/deps), 64 (usage) |
| `run-aws-large-benchmark.sh` | 1 (non-`aws-s3` endpoint, preflight-aws-ec2, G2), propagates 2/3 from ingest/bench |
| `run-tpuf-large-benchmark.sh` | 1 (preflight-tpuf, G2) |
| `ingest-large.sh` | 1 (tier/workload/upsert resume), **2** (serve), **3** (index) |
| `bench-large.sh` | 1 (gates/env), **2** (serve), **3** (index wait when used) |
| `verify-large-benchmark-program.sh` | 1 (any nested gate failed; child may use other codes) |

Other harness scripts (`validate-benchmark-json.sh`, `normalize-benchmark-json.sh --check`, `check-benchmark-artifacts.sh`) still use **0/1** only. Python drivers (`run_benchmark.py`, `run_spotcheck.py`) follow their own `sys.exit` conventions unless wrapped by the shell operators above.

## Scripts (one line each)

### Orchestration

| Entry | Description |
|-------|-------------|
| [Makefile](../Makefile) | `make bench-verify`, `make bench-dry-run`, `make bench-g2-minio`, `make bench-preflight` |
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
| [ingest-large.sh](../scripts/ingest-large.sh) | Sequential upsert batches (10k/batch, ~1.1s cadence) + retry/resume + index poll → `preferred_ann_version == 3`; `OPENPUFFER_INGEST_PARALLEL` must be 0 |
| [diagnose-index-lag.sh](../scripts/diagnose-index-lag.sh) | Poll namespace meta (`index_cursor` vs `wal_commit_seq`); use when ingest index wait times out |
| [bench-large.sh](../scripts/bench-large.sh) | Cold/filter/hybrid/warm queries → `large-aws-{tier}.json` |
| [render-report.sh](../scripts/render-report.sh) | Merge openpuffer + tpuf JSON → `docs/reports/BENCHMARK_VS_TURBOPUFFER_<date>.md` |

### Preflight

| Script | Description |
|--------|-------------|
| [install-benchmark-python-deps.sh](../scripts/install-benchmark-python-deps.sh) | One-shot `pip install -r benchmarks/requirements.txt` (pytest, jsonschema, turbopuffer, httpx) |
| [preflight-large-benchmark-all.sh](../scripts/preflight-large-benchmark-all.sh) | G3+G4+overlap preflights in one pass (`--live` on EC2; default offline `--dry-run` / `--skip-key`) |
| [preflight-aws-ec2.sh](../scripts/preflight-aws-ec2.sh) | EC2 IMDS, region/AZ, instance profile, S3 `head-bucket` before G3 |
| [preflight-tpuf.sh](../scripts/preflight-tpuf.sh) | API key, region vs AWS, RTT, cost estimate, results secret scan |
| [preflight-id-overlap.sh](../scripts/preflight-id-overlap.sh) | Phase 3.3 namespace/index checks before overlap spot-check |
| [estimate-large-benchmark-cost.sh](../scripts/estimate-large-benchmark-cost.sh) | Order-of-magnitude API volume before live G3/G4 (`--tier`, `--scope`, `--warm`) |
| [lib/large-benchmark-preflight.sh](../scripts/lib/large-benchmark-preflight.sh) | Shared tier/workload/MinIO→AWS path guards |

### Tests & CI helpers

| Script | Description |
|--------|-------------|
| [test_l2-l3-harness-dry-run.sh](../scripts/test_l2-l3-harness-dry-run.sh) | One-shot L2+L3 offline harness validation |
| [test_render-report.sh](../scripts/test_render-report.sh) | Offline render-report merge checks |
| [test_render-report-measured.sh](../scripts/test_render-report-measured.sh) | Measured-mode schema, interpretation, appendix redaction |
| [validate-benchmark-json.sh](../scripts/validate-benchmark-json.sh) | JSON Schema for fixtures + `*.example.json` (large-aws, tpuf, ingest, id-overlap) |
| [normalize-benchmark-json.sh](../scripts/normalize-benchmark-json.sh) | Canonical jq/Python pretty-print for committed benchmark JSON (`--check` for CI) |
| [check-benchmark-artifacts.sh](../scripts/check-benchmark-artifacts.sh) | Git policy: live `large-aws-*` must be `environment=aws-s3`; MinIO only in `*-schema-minio*` / legacy snapshots |
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
facts check --tags bench-op-scaling
```

- **Dispatch (dry-run):** [.github/workflows/benchmark-large-dispatch.yml](../.github/workflows/benchmark-large-dispatch.yml)
- **Nightly (G6):** [.github/workflows/nightly-stress.yml](../.github/workflows/nightly-stress.yml) — `large-dataset-program` (G2 @ 10k + l1 dry-run) runs **parallel** with `bench-100k` (L1 engine @ 100k MinIO; only one 100k ingest/night). See [docs/BENCHMARKS.md § G6 nightly](../docs/BENCHMARKS.md#github-actions-nightly-regression-g6).
- **G2 + 10k schema:** `g2-minio-correctness` in [.github/workflows/ci.yml](../.github/workflows/ci.yml)

## Further reading

- [CHANGELOG_LARGE_DATASET.md](CHANGELOG_LARGE_DATASET.md) — harness program commits (A1–A6, G2–G6) by date
- [docs/BENCHMARKS.md](../docs/BENCHMARKS.md) — SLOs, G2/G3/G4 runbooks, L2/L3 expectations
- [docs/COMPARISON.md](../docs/COMPARISON.md) — product comparison + measured L1 table (after live JSON)
- [docs/reports/BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md](../docs/reports/BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md) — report layout (`NOT MEASURED`)