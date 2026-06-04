# Operator log: live G3/G4 attempt (2026-06-04)

## G4 only — iteration 52 (2026-06-04)

| Check | Result |
|-------|--------|
| `TURBOPUFFER_API_KEY` | **unset** → G4 blocked |
| `./scripts/run-tpuf-large-benchmark.sh --tier l1` | **Skipped** (no API key) |
| `benchmarks/results/tpuf-l1.json` | **not** produced |
| Deliverable | [OPERATOR_RUNBOOK_QUICK.md](../OPERATOR_RUNBOOK_QUICK.md) G4 skip note |

---

## Retry — iteration 48 (`5627247`)

| Check | Result |
|-------|--------|
| `OPENPUFFER_S3_ENDPOINT` | `http://127.0.0.1:9000` → **minio** |
| `./scripts/run-aws-large-benchmark.sh --tier l1 --preflight-only` | G2 subset OK; **exit 1** — endpoint not AWS |
| Live `./scripts/run-aws-large-benchmark.sh --tier l1` | **Skipped** (MinIO only) |
| Deliverable | [OPERATOR_RUNBOOK_QUICK.md](../OPERATOR_RUNBOOK_QUICK.md) EC2 one-pager |

---

Session goal: run **G3** live AWS L1 (`large-aws-l1.json`) per `docs/PLAN_LARGE_DATASET_BENCHMARK.md`. Offline harness already complete at `bfaec74` — do not re-run `verify-large-benchmark-program.sh` for this unit.

## Environment detected (this host)

| Variable | Value |
|----------|--------|
| `OPENPUFFER_S3_ENDPOINT` | `http://127.0.0.1:9000` (**MinIO**, not AWS) |
| `OPENPUFFER_S3_BUCKET` | `openpuffer-integration` |
| `OPENPUFFER_S3_REGION` | `us-east-1` |
| `OPENPUFFER_S3_ACCESS_KEY` | `minioadmin` (dev MinIO) |
| EC2 (IMDS) | not available |
| `aws` CLI | not installed (head-bucket skipped) |
| `TURBOPUFFER_API_KEY` | **unset** (G4 blocked) |
| Git HEAD | `bfaec74` |

`large_preflight_detect_environment` → **`minio`**. G3 harness refuses non-`aws-s3` endpoints before ingest/bench spend.

## Preflight executed

| Step | Command | Result |
|------|---------|--------|
| EC2 + S3 | `./scripts/preflight-aws-ec2.sh` | OK (not on EC2; head-bucket skipped — no awscli) |
| S3 lib | `large_preflight_validate_s3_env` + `large_preflight_s3_head_bucket` | OK (weak; MinIO endpoint) |
| G3 gate | `./scripts/run-aws-large-benchmark.sh --tier l1 --preflight-only` | G2 subset **OK**; then **exit 1**: `OPENPUFFER_S3_ENDPOINT does not look like AWS (minio)` |
| G4 | `./scripts/preflight-tpuf.sh --tier l1 --skip-rtt` | **exit 1**: `TURBOPUFFER_API_KEY unset` |

Note: there is no separate `preflight-aws-s3` script; S3 validation lives in `scripts/preflight-aws-ec2.sh` and `scripts/lib/large-benchmark-preflight.sh` (`large_preflight_validate_s3_env`, `large_preflight_s3_head_bucket`).

## Live runs

| Goal | Ran? | Artifact |
|------|:----:|----------|
| **G3** `./scripts/run-aws-large-benchmark.sh --tier l1` | **No** | `large-aws-l1.json` **not** produced (MinIO env) |
| **G4** `./scripts/run-tpuf-large-benchmark.sh --tier l1` | **No** | `tpuf-l1.json` **not** produced (no API key) |

**Did not** commit MinIO timings as `benchmarks/results/large-aws-l1.json` (guard + plan policy). MinIO shape remains in `*-schema-minio*.example.json` only.

## Operator commands when creds exist (EC2 `m7i.xlarge`, `us-east-1`)

```bash
# On EC2 with instance profile + dedicated bench bucket:
unset OPENPUFFER_S3_ENDPOINT OPENPUFFER_S3_ACCESS_KEY OPENPUFFER_S3_SECRET_KEY  # let preflight derive AWS
export OPENPUFFER_S3_BUCKET=openpuffer-bench-<account>-us-east-1
export OPENPUFFER_S3_REGION=us-east-1
./scripts/preflight-aws-ec2.sh          # IMDS + head-bucket (install awscli)
./scripts/run-aws-large-benchmark.sh --tier l1
git add benchmarks/results/large-aws-l1.json benchmarks/results/ingest-large-l1.json

export TURBOPUFFER_API_KEY=<test-org-key>
export TURBOPUFFER_REGION=aws-us-east-1
./scripts/preflight-tpuf.sh --tier l1
./scripts/run-tpuf-large-benchmark.sh --tier l1

./scripts/run-large-benchmark-program.sh --tier l1 --measured-report
```

## Remaining work (program complete)

- [ ] Live `benchmarks/results/large-aws-l1.json` on real AWS S3
- [ ] Live `benchmarks/results/tpuf-l1.json`
- [ ] `id-overlap-l1.json` after both ingests
- [ ] Measured report + `docs/COMPARISON.md` L1 rows