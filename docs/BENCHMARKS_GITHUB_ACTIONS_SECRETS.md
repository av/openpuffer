# GitHub Actions — optional secrets for live large-dataset benchmarks

This document describes **repository secrets** for a future **manual** live comparison run (G3 AWS + G4 turbopuffer + G5 report) via GitHub Actions. It does **not** contain secret values.

**Default CI behavior (no secrets):**

| Workflow | Purpose |
|----------|---------|
| [`.github/workflows/benchmark-large-dispatch.yml`](../.github/workflows/benchmark-large-dispatch.yml) | Offline harness only — [`verify-large-benchmark-program.sh`](../scripts/verify-large-benchmark-program.sh) |
| [`.github/workflows/nightly-stress.yml`](../.github/workflows/nightly-stress.yml) job `large-dataset-program` | MinIO G2 + program dry-run |
| [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) job `g2-minio-correctness` | MinIO correctness + 10k schema fast path |

**Optional live workflow (secrets required, disabled by default):**

- [`.github/workflows/benchmark-large-live.yml`](../.github/workflows/benchmark-large-live.yml) — `workflow_dispatch` with `enable_live_run` default **`false`** (secrets preflight only until an operator explicitly enables a live run).

Operator runbooks for EC2 (recommended) remain in [BENCHMARKS.md § G3/G4](BENCHMARKS.md#large-dataset-program-operator-runbook-phases-4-6). GitHub-hosted `ubuntu-latest` is **not** a fairness substitute for same-region EC2; use Actions live runs only when you accept RTT skew or attach a **self-hosted runner** in the bench region.

---

## Where to configure secrets

1. GitHub repository → **Settings** → **Secrets and variables** → **Actions** → **Repository secrets**.
2. Use the **exact names** below so workflows can map them with `secrets.NAME` → `env.NAME`.
3. **Never** commit values, `.env` files, or live JSON containing keys. [`render-report.sh`](../scripts/render-report.sh) blocks measured merges if secrets appear in artifacts.

**Environments (optional):** create a `large-benchmark-live` environment with required reviewers and environment-scoped secrets for extra guardrails before `enable_live_run=true`.

---

## Required repository secrets (live G3 + G4)

| Secret name | Used by | Purpose |
|-------------|---------|---------|
| `OPENPUFFER_S3_BUCKET` | G3 ingest/bench | Dedicated bench bucket (`openpuffer-bench-<account>-<region>`) |
| `OPENPUFFER_S3_REGION` | G3, region alignment | AWS region (e.g. `us-east-1`) — must align with `TURBOPUFFER_REGION` |
| `OPENPUFFER_S3_ACCESS_KEY` | G3 S3 API | IAM user or role access key (**omit** if using OIDC/self-hosted instance profile only — see below) |
| `OPENPUFFER_S3_SECRET_KEY` | G3 S3 API | Matching secret key |
| `TURBOPUFFER_API_KEY` | G4 driver | **Test org** API key (`tpuf_…`); [Testing](https://turbopuffer.com/docs/testing) |

### Derived / recommended (not separate secrets)

Set these in the workflow `env:` block from secrets (do not store duplicate secrets):

| Variable | Typical value |
|----------|----------------|
| `OPENPUFFER_S3_ENDPOINT` | `https://s3.${OPENPUFFER_S3_REGION}.amazonaws.com` |
| `TURBOPUFFER_REGION` | Map from S3 region — e.g. `us-east-1` → `aws-us-east-1` ([`large_preflight_tpuf_region_for_s3`](../scripts/lib/large-benchmark-preflight.sh)) |
| `OPENPUFFER_ANN_VERSION` | `3` |
| `OPENPUFFER_COLD_S3_CONCURRENCY` | `32` (try `64` on AWS if RTT-bound) |
| `TURBOPUFFER_BENCH_DELETE_FIRST` | `1` (re-run hygiene) |

---

## Optional repository secrets / variables

| Name | Default if unset | Notes |
|------|------------------|-------|
| `OPENPUFFER_S3_ENDPOINT` | Derived from region | Override for S3-compatible endpoints (not for production comparison) |
| `TURBOPUFFER_REGION` | `aws-us-east-1` or mapped from `OPENPUFFER_S3_REGION` | Must match [region table](BENCHMARKS.md#g4-turbopuffer-operator-setup) |
| `OPENPUFFER_BENCH_HOST_LABEL` | `github-actions@ubuntu-latest` in live workflow | Record real host in measured JSON when using self-hosted EC2 |
| `OPENPUFFER_BENCH_CLIENT_MODE` | `localhost` on self-hosted; `remote` on hosted runners | Document in report methodology |
| `OPENPUFFER_BENCH_ENFORCE_GATES` | `1` for live | Set `0` only for debug |
| `OPENPUFFER_BENCH_ALLOW_MINIO_RESULTS` | unset | **Do not set** for live AWS comparison |
| `OPENPUFFER_INGEST_INDEX_TIMEOUT_SEC` | tier default (7200 / 10800 / 14400) | Raise if indexer lags |
| `OPENPUFFER_BENCH_INDEX_TIMEOUT_SEC` | tier default | Same |
| `TURBOPUFFER_BENCH_INDEX_TIMEOUT_SEC` | tier default | Same |
| `TURBOPUFFER_BENCH_SKIP_DELETE` | unset | Debug only — leaves billed tpuf storage |

Use **Variables** (non-secret) for non-sensitive toggles if preferred: `OPENPUFFER_BENCH_TIER` default, report date, etc.

---

## IAM policy sketch (G3 bucket)

Attach to the IAM principal behind `OPENPUFFER_S3_ACCESS_KEY` / `OPENPUFFER_S3_SECRET_KEY` (or OIDC role). Adjust account/region:

- `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on `arn:aws:s3:::openpuffer-bench-*` and `arn:aws:s3:::openpuffer-bench-*/*`
- Deny public ACLs on the bench bucket; dedicated prefix per namespace under `openpuffer/`

Full checklist: [BENCHMARKS.md § G3 — EC2 + AWS S3](BENCHMARKS.md#g3-ec2-aws-s3-operator-setup).

---

## turbopuffer guardrails (G4)

| Rule | Detail |
|------|--------|
| Test org only | Production keys must not enter GitHub secrets |
| Billing | Recall queries scale with tier (`num × ⌈docs/100k⌉`); start with **L1** |
| Cleanup | `TURBOPUFFER_BENCH_DELETE_FIRST=1`; driver `delete_all` in `finally` unless `SKIP_DELETE` |
| Namespace | Default `bench-tpuf-YYYY-MM-DD-{tier}`; override with `TURBOPUFFER_BENCH_NAMESPACE` for parallel runs |

Preflight: [`scripts/preflight-tpuf.sh`](../scripts/preflight-tpuf.sh).

---

## Workflow behavior matrix

| `enable_live_run` | Secrets present | Result |
|-------------------|-----------------|--------|
| `false` (default) | any | **secrets-preflight** job only — lists missing names, runs no ingest/bench |
| `true` | all required | **live-program** — `run-large-benchmark-program.sh` (G3→G4→overlap→report) |
| `true` | missing | Fails fast before AWS/tpuf spend |

Artifacts (when live succeeds): upload `benchmarks/results/large-aws-{tier}.json`, `tpuf-{tier}.json`, `id-overlap-{tier}.json`, and `docs/reports/BENCHMARK_VS_TURBOPUFFER_<date>.md` — **scrub** before committing to default branch if the workflow does not auto-redact.

---

## Alternatives to long-lived AWS keys in GitHub

| Approach | When to use |
|----------|-------------|
| **EC2 + instance profile** (recommended) | Run [`run-aws-large-benchmark.sh`](../scripts/run-aws-large-benchmark.sh) on EC2; [`preflight-aws-ec2.sh`](../scripts/preflight-aws-ec2.sh) — no keys in git or Actions |
| **Self-hosted Actions runner** on bench EC2 | Map instance profile to job env; omit `OPENPUFFER_S3_ACCESS_KEY` / `SECRET_KEY` secrets |
| **OIDC → AWS role** (future) | Replace static keys with `aws-actions/configure-aws-credentials` + trust policy; not wired in repo today |

`turbopuffer` always requires `TURBOPUFFER_API_KEY` in env (no instance-profile equivalent).

---

## Rotating and auditing secrets

1. Rotate `TURBOPUFFER_API_KEY` in the tpuf dashboard; update repository secret; re-run preflight workflow with `enable_live_run=false`.
2. Rotate AWS keys or role session; update secrets or instance profile; verify `aws s3api head-bucket` via [`preflight-aws-ec2.sh`](../scripts/preflight-aws-ec2.sh) or workflow preflight step.
3. After rotation, delete old namespaces (`bench-large-*`, `bench-tpuf-*`) if re-running comparison.
4. Audit: GitHub **Settings → Secrets** access log; never paste secrets into issue comments or workflow logs (`::add-mask::` is applied in the live workflow for key material).

---

## Related docs

- [BENCHMARKS.md — manual dry-run dispatch (A6)](BENCHMARKS.md#github-actions-manual-dry-run-preflight-a6)
- [BENCHMARKS.md — live workflow dispatch](BENCHMARKS.md#github-actions-optional-live-dispatch)
- [PLAN_LARGE_DATASET_BENCHMARK.md § Phase 8 A6](PLAN_LARGE_DATASET_BENCHMARK.md#phase-8--automation-roadmap-implementation-backlog)
- [COMPARISON.md](COMPARISON.md) — measured rows after live JSON exists