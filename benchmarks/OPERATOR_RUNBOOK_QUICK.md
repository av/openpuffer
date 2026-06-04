# EC2 operator quick runbook (G3 → G5)

One-page checklist for **live** `large-aws-*.json` / `tpuf-*.json`. Full IAM, troubleshooting, and tier tables: [docs/BENCHMARKS.md § G3/G4](../docs/BENCHMARKS.md#g3--ec2--aws-s3-operator-setup).

## Before you spend

| Check | Why |
|-------|-----|
| **Not MinIO** | `OPENPUFFER_S3_ENDPOINT` must be `*amazonaws.com*` (or unset → derived). `127.0.0.1:9000` / `minioadmin` → G3 guard exits. |
| **Dedicated bench bucket** | e.g. `openpuffer-bench-<account>-us-east-1` — never production data. |
| **EC2 in bucket region** | `m7i.xlarge` in `us-east-1` (default); attach instance profile. |
| **Local G2 green** | `./scripts/run-minio-correctness-gates.sh` on laptop/CI before AWS ingest. |

## 5-minute env (on EC2)

```bash
git clone <repo> && cd openpuffer
cargo build --release --features integration   # or copy release binary
dnf install -y jq curl python3 awscli          # or apt equivalent

# Clear dev MinIO — required for G3
unset OPENPUFFER_S3_ENDPOINT OPENPUFFER_S3_ACCESS_KEY OPENPUFFER_S3_SECRET_KEY
export OPENPUFFER_S3_BUCKET=openpuffer-bench-<account>-us-east-1
export OPENPUFFER_S3_REGION=us-east-1
# optional explicit endpoint:
# export OPENPUFFER_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com

./scripts/preflight-aws-ec2.sh                    # IMDS, region, head-bucket, session keys
```

## L1 measured run (copy-paste)

```bash
./scripts/run-aws-large-benchmark.sh --preflight-only --tier l1
./scripts/run-aws-large-benchmark.sh --tier l1
# → benchmarks/results/large-aws-l1.json (+ ingest-large-l1.json)

export TURBOPUFFER_API_KEY=<test-org-key>
export TURBOPUFFER_REGION=aws-us-east-1
./scripts/preflight-tpuf.sh --tier l1
./scripts/run-tpuf-large-benchmark.sh --tier l1
# → benchmarks/results/tpuf-l1.json

./scripts/run-id-overlap-spotcheck.sh --tier l1
# → benchmarks/results/id-overlap-l1.json

./scripts/run-large-benchmark-program.sh --tier l1 --measured-report
# or: ./scripts/render-report.sh --date $(date +%F)
```

## Commit policy

| Commit | Do not commit |
|--------|----------------|
| `large-aws-l1.json`, `ingest-large-l1.json` | MinIO timings as `large-aws-l1.json` |
| `tpuf-l1.json`, `id-overlap-l1.json` | `TURBOPUFFER_API_KEY`, `.env`, IAM secrets |
| `docs/reports/BENCHMARK_VS_TURBOPUFFER_<date>.md` | `*-schema-minio*.example.json` as “measured AWS” |

Validate before push:

```bash
./scripts/validate-benchmark-json.sh benchmarks/results/large-aws-l1.json
./scripts/check-benchmark-artifacts.sh --staged
git add -f benchmarks/results/large-aws-l1.json benchmarks/results/tpuf-l1.json
```

## Wall-clock (L1 @ 100k, m7i.xlarge)

Plan **≥45–90 min** first run: WAL ingest ~15 min + index catch-up often **15–60 min** + bench ~10 min. See `large_preflight_aws_time_estimate` in preflight output.

## If blocked on a dev laptop

MinIO / no tpuf key is expected — log in [results/OPERATOR_G3_G4_ATTEMPT.md](results/OPERATOR_G3_G4_ATTEMPT.md). Use MinIO schema path only: `./scripts/run-minio-large-schema-example.sh --tier l1`.

### G4 skipped when `TURBOPUFFER_API_KEY` unset (2026-06-04)

Live G4 was **not** run on this host: `TURBOPUFFER_API_KEY` was unset at operator time. Do **not** commit fixture or dry-run JSON as `benchmarks/results/tpuf-l1.json`.

When the key is available on EC2 (same region as openpuffer bench):

```bash
export TURBOPUFFER_API_KEY=<test-org-key>   # never commit; see turbopuffer.com/docs/testing
export TURBOPUFFER_REGION=aws-us-east-1
./scripts/preflight-tpuf.sh --tier l1
./scripts/run-tpuf-large-benchmark.sh --tier l1
./scripts/preflight-tpuf.sh --check-results benchmarks/results/tpuf-l1.json
git add benchmarks/results/tpuf-l1.json
```

## Program complete

- [ ] `large-aws-l1.json` (`environment` ≠ `minio`)
- [ ] `tpuf-l1.json`
- [ ] `id-overlap-l1.json`
- [ ] Measured report + [COMPARISON.md](../docs/COMPARISON.md) L1 rows

Harness-only status: [PLAN_LARGE_DATASET_BENCHMARK.md](../docs/PLAN_LARGE_DATASET_BENCHMARK.md).