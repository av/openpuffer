# Benchmarks

Measurable baselines and scale gates for [PLAN_SPFRESH_AND_COLD_1M.md](PLAN_SPFRESH_AND_COLD_1M.md). Work is fact-driven: `@spec` facts under `index/ann` and `query/cold` in `.facts` are checked with `facts check --tags cold,ann`.

## Large-dataset program — G2 correctness gates (MinIO)

Before AWS/turbopuffer comparison runs ([`PLAN_LARGE_DATASET_BENCHMARK.md`](PLAN_LARGE_DATASET_BENCHMARK.md) Phase 2–3), prove API semantics on the **shared synthetic-128 fixture** (`benchmarks/workloads/synthetic-128/l1-100k/`).

| Gate | Command | What it checks |
|------|---------|----------------|
| Fixture vectors | `cargo test --test synthetic_workload_gate` | `queries.json` vectors match `bench_sin_v1`; `recall_defaults` num=20, top_k=10 |
| Integration smoke | `cargo test -F integration --test integration_s3 synthetic_128_g2_correctness_gates_on_minio` | 10k ingest with workload schema; `/recall`, all 6 filter + 4 hybrid queries, cold vector from `queries.json` |
| Bench cold | `cargo test -F bench --test bench_cold bench_cold_10k_synthetic_128_workload_gate` | Same workload on bench path; recall ≥ 0.85; `storage_roundtrips ≤ 4` |

**One-shot preflight** (subset; fast path for Phase 2.3):

```bash
./scripts/run-minio-correctness-gates.sh
```

**CI:** On every push/PR, job `g2-minio-correctness` in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs the same script (Docker testcontainers), then compose MinIO + [`run-minio-large-schema-example.sh`](../scripts/run-minio-large-schema-example.sh) `--docs 10000` (schema fast path, 25m step timeout).

**Full MinIO preflight** (plan §2.3 — longer):

```bash
cargo test -F integration --test integration_s3 -- --nocapture
cargo test -F bench --test bench_cold -- --nocapture
cargo test --release -F bench --test bench_cold -- --ignored --nocapture   # 100k nightly
```

Helpers live in [`tests/common/synthetic_workload.rs`](../tests/common/synthetic_workload.rs). Ingest/query scripts use the same manifest via [`scripts/ingest-large.sh`](../scripts/ingest-large.sh) and [`scripts/bench-large.sh`](../scripts/bench-large.sh).

---

## Large-dataset program — Operator runbook (Phases 4–6)

Performance measurement, debugging, and pass/fail assessment for the [large-dataset comparison program](PLAN_LARGE_DATASET_BENCHMARK.md). **MinIO timings are not used in the turbopuffer report** — AWS + managed tpuf only for latency comparison.

### End-to-end operator flow

**One command** (chains G2 → G3 → G4 → id-overlap → G5 report dry-run; documents AWS + tpuf env):

```bash
./scripts/run-large-benchmark-program.sh --dry-run          # plan + fixture report (no creds)
./scripts/run-large-benchmark-program.sh --tier l1          # live when OPENPUFFER_S3_* + TURBOPUFFER_API_KEY set
./scripts/run-large-benchmark-program.sh --tier l1 --warm   # adds warm/filter/hybrid secondary on both sides
./scripts/run-large-benchmark-program.sh --preflight-only     # G2 + AWS/tpuf preflight only
./scripts/run-large-benchmark-program.sh --aws-only --tier l1 # G3 only
./scripts/run-large-benchmark-program.sh --measured-report  # render-report without --dry-run (after JSON committed)
```

See [`scripts/run-large-benchmark-program.sh`](../scripts/run-large-benchmark-program.sh) `--help` for flags (`--skip-g2`, `--full-g2`, `--skip-tpuf`, etc.).

| Step | Phase | Command | Output |
|------|-------|---------|--------|
| 0 | G2 | [`scripts/run-minio-correctness-gates.sh`](../scripts/run-minio-correctness-gates.sh) | Block AWS/tpuf spend if red |
| 0b | G3 preflight | [`scripts/run-aws-large-benchmark.sh`](../scripts/run-aws-large-benchmark.sh) `--preflight-only` | G2 subset + AWS `head-bucket` + workload manifest |
| 0c | G4 preflight | [`scripts/preflight-tpuf.sh`](../scripts/preflight-tpuf.sh) `--tier l1` or `run-tpuf-large-benchmark.sh --preflight-only` | API key, region vs S3/EC2, RTT, cost estimate, workload manifest |
| 1 | ingest | [`scripts/ingest-large.sh`](../scripts/ingest-large.sh) `--tier l1` | Namespace on AWS S3, `preferred_ann_version == 3` |
| 2 | bench | [`scripts/bench-large.sh`](../scripts/bench-large.sh) `--tier l1` | `benchmarks/results/large-aws-l1.json` |
| 3 | tpuf | [`scripts/run-tpuf-large-benchmark.sh`](../scripts/run-tpuf-large-benchmark.sh) `--tier l1` | `benchmarks/results/tpuf-l1.json` |
| 3b | 3.3 | [`scripts/run-id-overlap-spotcheck.sh`](../scripts/run-id-overlap-spotcheck.sh) `--tier l1` | `benchmarks/results/id-overlap-l1.json` (after both sides indexed) |
| 4 | report | [`scripts/render-report.sh`](../scripts/render-report.sh) | `docs/reports/BENCHMARK_VS_TURBOPUFFER_<date>.md` |

**Fairness:** run the tpuf driver from the **same region** as the openpuffer S3 bucket and tpuf namespace. Record client RTT before interpreting cold p50 deltas ([plan § architecture](PLAN_LARGE_DATASET_BENCHMARK.md#architecture-of-the-evaluation)).

**G3 one-shot (EC2 + AWS credentials):**

```bash
export OPENPUFFER_S3_BUCKET=openpuffer-bench-<account>-us-east-1
export OPENPUFFER_S3_REGION=us-east-1
export OPENPUFFER_ANN_VERSION=3
export OPENPUFFER_COLD_S3_CONCURRENCY=32

# On EC2: instance profile + metadata/region/S3 checks (no long-lived keys in shell history)
./scripts/preflight-aws-ec2.sh
# Off EC2: set OPENPUFFER_S3_ACCESS_KEY / OPENPUFFER_S3_SECRET_KEY, then:
# export OPENPUFFER_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com

./scripts/run-aws-large-benchmark.sh --tier l1
# preflight only: ./scripts/run-aws-large-benchmark.sh --preflight-only --tier l1
```

See [§ G3 — EC2 + AWS S3 operator setup](#g3--ec2--aws-s3-operator-setup) for instance type, IAM, security, first-time checklist, and index-lag troubleshooting.

**G4 one-shot (API key in same region as AWS bench):**

```bash
export TURBOPUFFER_API_KEY=tpuf_...
export TURBOPUFFER_REGION=aws-us-east-1   # match OPENPUFFER_S3_REGION / EC2
export TURBOPUFFER_BENCH_DELETE_FIRST=1   # clear namespace before ingest (re-runs)

./scripts/preflight-tpuf.sh --tier l1
./scripts/run-tpuf-large-benchmark.sh --tier l1
# preflight only: ./scripts/run-tpuf-large-benchmark.sh --preflight-only --tier l1
```

See [§ G4 — turbopuffer operator setup](#g4--turbopuffer-operator-setup) for test org, billing guardrails, and artifact redaction.

Shared S3/tpuf/workload checks live in [`scripts/lib/large-benchmark-preflight.sh`](../scripts/lib/large-benchmark-preflight.sh). `bench-large.sh` refuses to write `large-aws-*.json` from a MinIO endpoint unless `OPENPUFFER_BENCH_ALLOW_MINIO_RESULTS=1` or the results path contains `minio` / `example` / `schema`.

**MinIO schema example only** (validates `cold_large_l1` JSON shape; **not** for COMPARISON / tpuf):

```bash
./scripts/run-minio-large-schema-example.sh
# → large-aws-l1-schema-minio.example.json + ingest-large-l1-schema-minio.example.json
#   (environment=minio; ingest timing; filter/hybrid; warm by default; --skip-warm optional)

# CI / quick schema validation (~2–5 min; committed 10k exemplars, not the 100k artifacts):
./scripts/run-minio-large-schema-example.sh --docs 10000
# → large-aws-l1-schema-minio-10k.example.json + ingest-large-l1-schema-minio-10k.example.json
```

**Dry-run** (no credentials):

```bash
./scripts/run-large-benchmark-program.sh --dry-run --tier l1   # or l2 / l3
./scripts/run-aws-large-benchmark.sh --dry-run --tier l2
./scripts/run-tpuf-large-benchmark.sh --dry-run --tier l3
./scripts/ingest-large.sh --tier l1 --dry-run
./scripts/bench-large.sh --tier l2 --dry-run
python3 benchmarks/tpuf_driver/run_benchmark.py --tier l3 --dry-run
./scripts/run-id-overlap-spotcheck.sh --tier l2 --dry-run
./scripts/run-id-overlap-spotcheck.sh --tier l1 --mock
./scripts/render-report.sh --dry-run --tier l2
./scripts/test_l2-l3-harness-dry-run.sh   # one-shot L2+L3 gate (CI / local)
```

Tiers: **l1** (100k, default comparison), **l2** (500k), **l3** (1M). Workloads under `benchmarks/workloads/synthetic-128/{l1-100k,l2-500k,l3-1m}/`.

### L2 / L3 scale tiers — operator expectations

Run **L1 first** on AWS + tpuf before L2/L3 spend. MinIO proves correctness only (G2); **do not** publish MinIO latencies in COMPARISON / tpuf reports.

| Tier | Docs | Namespace default | WAL ingest (10k batches, ~1.1s sleep) | Index poll default | Full G3 on `m7i.xlarge` (typical) | tpuf recall billed (`num=20`) |
|------|------|-------------------|---------------------------------------|--------------------|-----------------------------------|-------------------------------|
| **L1** | 100k | `bench-large-100000` | ~12–15 min | `7200`s (2h) | ~30–90 min | ~20 |
| **L2** | 500k | `bench-large-500000` | ~60–70 min | `10800`s (3h) | ~2–4 h | ~100 |
| **L3** | 1M | `bench-large-1000000` | ~17–20 min WAL only | `14400`s (4h) | ~3–6 h | ~200 |

**Index timeouts:** when `OPENPUFFER_INGEST_INDEX_TIMEOUT_SEC` / `OPENPUFFER_BENCH_INDEX_TIMEOUT_SEC` / `TURBOPUFFER_BENCH_INDEX_TIMEOUT_SEC` are unset, harnesses pick tier defaults above (`large_preflight_tier_index_timeout_sec` in [`scripts/lib/large-benchmark-preflight.sh`](../scripts/lib/large-benchmark-preflight.sh)). Raise on slow hosts or if `index_cursor` lags after ingest.

**Cost / API volume (tpuf):** same cold/filter/hybrid query count as L1; recall billing scales with docs ([§ G4 billing](#billing-and-cost-guardrails-l1--l2--l3)). L3 optional `recall_defaults.num=10` with methodology note in the report.

**End-to-end dry-run (offline):**

```bash
./scripts/test_l2-l3-harness-dry-run.sh
./scripts/run-large-benchmark-program.sh --dry-run --tier l2 --skip-g2
./scripts/run-large-benchmark-program.sh --dry-run --tier l3 --skip-g2
```

**GitHub Actions:** [benchmark-large-dispatch.yml](../.github/workflows/benchmark-large-dispatch.yml) `workflow_dispatch` runs ingest/bench/tpuf/program/id-overlap dry-run for the selected tier (`l1`, `l2`, or `l3`).

**Spot-check ids:** `queries.json` `spot_check` uses the first 10 `vector_queries`; doc indices scale with tier (`num_docs / 50` stride), e.g. L2 → 0, 10k, …, 90k; L3 → 0, 20k, …, 180k.

### G3 — EC2 + AWS S3 operator setup

Live G3 comparison JSON (`large-aws-{tier}.json`) must be produced on **real AWS S3** from a host in the **same region** as the bucket. MinIO timings stay in G2/schema examples only.

#### Recommended EC2 instance

| Choice | When to use | Why |
|--------|-------------|-----|
| **`m7i.xlarge`** (default) | L1–L3 ingest + bench + `serve` on one host | 4 vCPU Intel Sapphire Rapids, 16 GiB RAM — enough for v3 index builds @ 100k–1M, rerank decode, and parallel cold `GetObject` without swapping |
| `c7i.xlarge` | Cost-sensitive L1 only | Compute-optimized; slightly less RAM headroom for 1M index objects |
| `c6i.large` | Minimal L1 smoke | 2 vCPU / 4 GiB — OK for 100k if you accept longer `index_wait_sec`; avoid for L3 |

**Placement:** launch in the **same region and AZ** as the S3 bucket (e.g. `us-east-1` + `us-east-1a`). Record the label in results:

```bash
export OPENPUFFER_BENCH_HOST_LABEL=m7i.xlarge@us-east-1a   # auto-set by preflight-aws-ec2.sh on EC2
export OPENPUFFER_BENCH_CLIENT_MODE=localhost                 # serve + curl bench on same instance (recommended)
```

**Not recommended:** burstable `t3`/`t4g` for index catch-up (CPU credits stall indexer during L1 ingest).

#### S3 bucket (same region)

1. Create a **dedicated** bucket, e.g. `openpuffer-bench-<account-id>-us-east-1`, in the target region (default **`us-east-1`**).
2. Do **not** reuse production data buckets; comparison writes under `s3://<bucket>/openpuffer/<namespace>/`.
3. Optional: enable versioning for forensics; lifecycle rule to expire `openpuffer/bench-*` prefixes after N days.
4. Block public access (account default).

```bash
export OPENPUFFER_S3_BUCKET=openpuffer-bench-123456789012-us-east-1
export OPENPUFFER_S3_REGION=us-east-1
export OPENPUFFER_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com
```

#### IAM — least privilege on the bench bucket only

Attach an **instance profile** to the EC2 role (preferred). The role needs **read/write on the bench bucket only** — not `s3:*` on `*`. Do **not** grant this role read access to production buckets (“bench-only” scope).

Replace `BENCH_BUCKET` and `ACCOUNT_ID`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BenchBucketList",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::BENCH_BUCKET"
    },
    {
      "Sid": "BenchObjectRW",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::BENCH_BUCKET/*"
    }
  ]
}
```

**Preflight-only** (no ingest): `s3:ListBucket` + `s3:GetBucketLocation` suffice for [`preflight-aws-ec2.sh`](../scripts/preflight-aws-ec2.sh) `head-bucket`. Full G3 ingest/bench requires the object RW statement above.

Trust policy for EC2:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "ec2.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
```

#### Security — no keys in git

| Rule | Detail |
|------|--------|
| **Never commit** | `OPENPUFFER_S3_*`, `TURBOPUFFER_API_KEY`, or `.env` with secrets |
| **Prefer instance profile** | IAM role on EC2; [`preflight-aws-ec2.sh`](../scripts/preflight-aws-ec2.sh) exports short-lived keys for `openpuffer serve` via `aws configure export-credentials` |
| **SSH only** | No public `serve` ingress; bench client hits `127.0.0.1:8080` |
| **CI** | A6 / nightly stay dry-run; live AWS stays operator-owned |
| **Artifacts** | `render-report.sh` redacts keys in reports; [`preflight-tpuf.sh --check-results`](../scripts/preflight-tpuf.sh) scans `tpuf-*.json` before commit |

`openpuffer serve` currently requires explicit `--s3-access-key` / `--s3-secret-key` (static credential pair). On EC2 with a role, run `./scripts/preflight-aws-ec2.sh` before ingest so session keys are populated — do not bake long-lived IAM user keys into AMIs.

#### First-time operator checklist (G3)

| # | Step | Command / note |
|---|------|----------------|
| 1 | Local/CI G2 green | `./scripts/run-minio-correctness-gates.sh` |
| 2 | Clone repo on EC2 | `git clone … && cd openpuffer` |
| 3 | Toolchain | `dnf install -y gcc curl jq python3 awscli` (or `apt`); `rustup` + `cargo build --release --features integration` |
| 4 | Create bench bucket + IAM role | Same region as EC2; attach instance profile |
| 5 | Env | `export OPENPUFFER_S3_BUCKET=… OPENPUFFER_S3_REGION=…` |
| 6 | EC2 preflight | `./scripts/preflight-aws-ec2.sh` (IMDS, region match, head-bucket, host label) |
| 7 | Program preflight | `./scripts/run-aws-large-benchmark.sh --preflight-only --tier l1` |
| 8 | L1 measured run | `./scripts/run-aws-large-benchmark.sh --tier l1` → `benchmarks/results/large-aws-l1.json` |
| 9 | G4 tpuf preflight | `./scripts/preflight-tpuf.sh --tier l1` (key, region, RTT, cost estimate) |
| 10 | G4 tpuf measured | `export TURBOPUFFER_API_KEY=…`; `./scripts/run-tpuf-large-benchmark.sh --tier l1` → `tpuf-l1.json` |
| 11 | Artifact scan | `./scripts/preflight-tpuf.sh --check-results benchmarks/results/tpuf-l1.json` |
| 12 | Report | Commit JSON; `./scripts/render-report.sh --date $(date +%F)`; update [COMPARISON.md](COMPARISON.md) |

**Wall-clock hints (L1 @ 100k):** ingest upsert ~2–3 min (WAL ~1 commit/s) + index wait often **10–30 min** on `m7i.xlarge` (depends on CPU and S3 PUT rate). Plan **≥45 min** before `bench-large` for first run.

#### Troubleshooting — index lag on real S3

Symptom: `ingest-large.sh` or `bench-large.sh` times out waiting for `index_cursor == wal_commit_seq` and `preferred_ann_version == 3`.

| Check | Command | Interpretation |
|-------|---------|----------------|
| Meta lag | `curl -s "http://127.0.0.1:8080/v1/namespaces/${NAMESPACE}" \| jq '{index_cursor,wal_commit_seq,unindexed_bytes,preferred_ann_version}'` | `index_cursor < wal_commit_seq` → indexer still draining WAL |
| S3 index growth | `aws s3 ls "s3://${OPENPUFFER_S3_BUCKET}/openpuffer/${NAMESPACE}/index/" \| wc -l` | Stuck count → indexer not committing; check `serve` logs |
| CPU | `top` / `htop` on EC2 | 100% CPU + burstable instance → switch to `m7i.xlarge` |
| Region | `./scripts/preflight-aws-ec2.sh` | EC2 region ≠ bucket region multiplies round-trips |
| Timeout | `export OPENPUFFER_INGEST_INDEX_TIMEOUT_SEC=3600` (ingest) or `OPENPUFFER_BENCH_INDEX_TIMEOUT_SEC=3600` (bench) | L1 default poll timeout 7200s; raise if needed |
| Re-run ingest poll only | Namespace exists; upsert done | `./scripts/ingest-large.sh --tier l1` (skips if caught up) or `bench-large` index wait |
| v2 index mistake | Meta `preferred_ann_version` | Must be **3**; `export OPENPUFFER_ANN_VERSION=3` before `serve`; delete namespace and re-ingest if v2 |
| 503 SlowDown | S3 API errors in logs | Lower parallel index writes; retry; verify IAM not throttling |

**Indexer not running:** `bench-large.sh` starts `openpuffer serve` unless `OPENPUFFER_BENCH_SKIP_SERVE=1`. If you run `serve` manually, keep one process per namespace during index catch-up.

**After catch-up:** confirm `index_cursor_eq_wal_commit_seq: true` in `ingest-large-l1.json` / `large-aws-l1.json` before trusting cold p50.

#### EC2 preflight script

[`scripts/preflight-aws-ec2.sh`](../scripts/preflight-aws-ec2.sh) — validates:

- IMDSv2 instance id, type, AZ, region (when on EC2)
- `OPENPUFFER_S3_REGION` matches EC2 placement region
- `aws s3api head-bucket` (or warns if CLI missing)
- Optional auto `OPENPUFFER_BENCH_HOST_LABEL` / `OPENPUFFER_BENCH_CLIENT_MODE`
- Instance-profile credential export when keys unset

```bash
./scripts/preflight-aws-ec2.sh
source <(./scripts/preflight-aws-ec2.sh --export-creds)   # manual shell bootstrap
./scripts/preflight-aws-ec2.sh --warn-only                # region mismatch → warning only
```

[`run-aws-large-benchmark.sh`](../scripts/run-aws-large-benchmark.sh) invokes this automatically before AWS env validation.

### G4 — turbopuffer operator setup

Live G4 comparison JSON (`tpuf-{tier}.json`) must come from **managed turbopuffer** in the **same region** as the openpuffer AWS bench host (typically the G3 EC2 instance). Do not use MinIO timings or a mismatched tpuf region when interpreting cold p50.

#### Dedicated test org and namespace isolation

Per [turbopuffer Testing](https://turbopuffer.com/docs/testing):

| Rule | Detail |
|------|--------|
| **Separate org** | Create a **dedicated test organization** in the turbopuffer dashboard — not production. Issue one API key per operator host. |
| **Key storage** | `export TURBOPUFFER_API_KEY=tpuf_…` on the bench host only; never commit, log, or paste into JSON artifacts. |
| **Namespace pattern** | Default `bench-tpuf-YYYY-MM-DD-{tier}` (`run_benchmark.py`); override with `TURBOPUFFER_BENCH_NAMESPACE` for parallel runs. |
| **Ephemeral** | Namespace create is cheap; **delete after each run** (`delete_all` in driver `finally`). |

```bash
export TURBOPUFFER_API_KEY=tpuf_...          # test org key — never commit
export TURBOPUFFER_REGION=aws-us-east-1      # see region table below
export TURBOPUFFER_BENCH_DELETE_FIRST=1      # delete namespace before ingest (re-runs)
```

#### Region alignment with AWS bench (required)

| `OPENPUFFER_S3_REGION` (AWS) | `TURBOPUFFER_REGION` (tpuf) | API host |
|------------------------------|----------------------------|----------|
| `us-east-1` | `aws-us-east-1` | `aws-us-east-1.turbopuffer.com` |
| `us-east-2` | `aws-us-east-2` | `aws-us-east-2.turbopuffer.com` |
| `us-west-2` | `aws-us-west-2` | `aws-us-west-2.turbopuffer.com` |
| `eu-west-1` | `aws-eu-west-1` | `aws-eu-west-1.turbopuffer.com` |
| `eu-central-1` | `aws-eu-central-1` | `aws-eu-central-1.turbopuffer.com` |

**Fairness:** run G4 from the **same EC2** (or same AZ/region) as G3. [`preflight-tpuf.sh`](../scripts/preflight-tpuf.sh) compares `TURBOPUFFER_REGION` to `OPENPUFFER_S3_REGION` and EC2 IMDS placement, and probes `curl` connect time to `{region}.turbopuffer.com`. If connect RTT **> ~50 ms**, fix region before trusting latency ratios.

[`preflight-aws-ec2.sh`](../scripts/preflight-aws-ec2.sh) + [`preflight-tpuf.sh`](../scripts/preflight-tpuf.sh) should both pass on the bench host before live spend.

#### Billing and cost guardrails (L1 / L2 / L3)

Order-of-magnitude **API volume** for one full driver run (not USD — check the turbopuffer dashboard). Recall billing follows [Recall billing](https://turbopuffer.com/docs/recall#billing): billed queries ≈ `max(num, num × ⌈docs / 100k⌉)` when `avg_recall ≥ 0.9`.

| Tier | Docs | Write batches (~10k) | Cold | Filter+hybrid | Recall billed (`num=20`) | Optional `--warm` |
|------|------|----------------------|------|---------------|--------------------------|-------------------|
| **L1** | 100k | ~10 | 7 | 10 | ~20 | +20 vector queries |
| **L2** | 500k | ~50 | 7 | 10 | ~100 | +20 |
| **L3** | 1M | ~100 | 7 | 10 | ~200 | +20 |

**Operator guardrails:**

| Guardrail | Env / action |
|-----------|----------------|
| Start at **L1** | Prove gates + report pipeline before L2/L3 spend |
| **DELETE_FIRST** | `TURBOPUFFER_BENCH_DELETE_FIRST=1` (default in `run-tpuf-large-benchmark.sh`) — `delete_all` before ingest on re-runs |
| **DELETE after** | Driver `finally` calls `delete_all` unless `TURBOPUFFER_BENCH_SKIP_DELETE=1` (debug only; leaves storage billed) |
| **Recall cost** | L1/L2: `recall_defaults` `num=20`; L3: consider `num=10` with methodology note |
| **No SKIP_DELETE in CI** | Never set `SKIP_DELETE` in automation |

Print tier estimate: `./scripts/preflight-tpuf.sh --tier l1` (add `--warm` for warm-line estimate).

#### API key redaction in artifacts

| Surface | Redaction |
|---------|-----------|
| **Reports** | [`render-report.sh`](../scripts/render-report.sh) (measured mode) validates JSON schema + workload alignment, blocks merge if secrets remain in artifacts, auto-writes comparison interpretation (plan §6.2), and embeds redacted JSON in the appendix; redacts `tpuf_*` keys, `TURBOPUFFER_API_KEY=`, `OPENPUFFER_S3_SECRET_KEY=` |
| **JSON results** | `tpuf-*.json` must **not** contain API keys (driver never writes them). Before `git add`: `./scripts/preflight-tpuf.sh --check-results benchmarks/results/tpuf-l1.json` |
| **Shell history** | Prefer env exports on EC2; do not echo keys in `notes` fields |

If a key was accidentally pasted into JSON, re-run the benchmark or scrub with the same patterns as `render-report.sh` before commit.

#### First-time operator checklist (G4)

| # | Step | Command / note |
|---|------|----------------|
| 1 | G2 green | `./scripts/run-minio-correctness-gates.sh` |
| 2 | Test org + API key | Dashboard → dedicated org → API key in env only |
| 3 | Region | `TURBOPUFFER_REGION` matches [table above](#region-alignment-with-aws-bench-required) and G3 `OPENPUFFER_S3_REGION` |
| 4 | tpuf preflight | `./scripts/preflight-tpuf.sh --tier l1` |
| 5 | Program preflight | `./scripts/run-tpuf-large-benchmark.sh --preflight-only --tier l1` |
| 6 | L1 measured | `./scripts/run-tpuf-large-benchmark.sh --tier l1` → `benchmarks/results/tpuf-l1.json` |
| 7 | Artifact scan | `./scripts/preflight-tpuf.sh --check-results benchmarks/results/tpuf-l1.json` |
| 8 | Overlap + report | After G3 JSON: `run-id-overlap-spotcheck.sh`; `render-report.sh` |

**Wall-clock hints (L1 @ 100k):** ingest often **1–5 min** (10× 10k writes); index wait varies; cold+recall+secondary queries are minutes, not hours. L3 ingest+recall can be **tens of minutes** and **~200** billed recall queries — run only after L1 is green.

#### turbopuffer preflight script

[`scripts/preflight-tpuf.sh`](../scripts/preflight-tpuf.sh) — validates:

- `TURBOPUFFER_API_KEY` present (prints `tpuf_` prefix only)
- `TURBOPUFFER_REGION` defaults from `OPENPUFFER_S3_REGION` when unset
- EC2 placement region vs tpuf region (when on EC2)
- Workload `manifest.json` / `queries.json` for tier
- Python `turbopuffer` package
- Cost estimate + DELETE_FIRST / SKIP_DELETE warnings
- Optional RTT to `{region}.turbopuffer.com`

```bash
./scripts/preflight-tpuf.sh --tier l1
./scripts/preflight-tpuf.sh --tier l3 --warm              # include warm query estimate
./scripts/preflight-tpuf.sh --tier l1 --warn-only           # region mismatch → warning
./scripts/preflight-tpuf.sh --skip-rtt                      # offline key/region/workload
./scripts/preflight-tpuf.sh --check-results benchmarks/results/tpuf-l1.json
```

[`run-tpuf-large-benchmark.sh`](../scripts/run-tpuf-large-benchmark.sh) invokes this automatically before `run_benchmark.py` (unless dry-run).

### GitHub Actions — manual dry-run preflight (A6)

Not scheduled on push/PR (AWS/tpuf cost). Use when validating harness changes before a live comparison run.

1. GitHub → **Actions** → **Large-dataset benchmark (dispatch)** → **Run workflow**.
2. Choose **tier** (`l1`, `l2`, or `l3`; default `l1`).
3. Workflow runs offline gates only (no repository secrets required in this iteration):
   - `pytest benchmarks/workloads/test_generate_synthetic.py`
   - `pytest benchmarks/tpuf_driver/test_run_benchmark.py`
   - [`scripts/test_render-report.sh`](../scripts/test_render-report.sh)
   - [`scripts/test_render-report-measured.sh`](../scripts/test_render-report-measured.sh) (fixture merge simulating live G5)
   - [`scripts/ingest-large.sh`](../scripts/ingest-large.sh) / [`scripts/bench-large.sh`](../scripts/bench-large.sh) `--dry-run` for the selected tier
   - [`scripts/run-tpuf-large-benchmark.sh`](../scripts/run-tpuf-large-benchmark.sh) `--dry-run` (wraps tpuf driver)
   - [`benchmarks/tpuf_driver/run_benchmark.py`](../benchmarks/tpuf_driver/run_benchmark.py) `--dry-run`
   - `facts check --tags bench-large` and `facts check --tags bench-tpuf`

Workflow file: [`.github/workflows/benchmark-large-dispatch.yml`](../.github/workflows/benchmark-large-dispatch.yml). Live ingest/bench on AWS and managed turbopuffer still run from an operator host with credentials (future: optional job inputs + secrets for one-click live runs).

### GitHub Actions — nightly regression (G6)

Scheduled with [`.github/workflows/nightly-stress.yml`](../.github/workflows/nightly-stress.yml) (**03:00 UTC**, same cron as 100k bench). Job **`large-dataset-program`** (no repository secrets):

1. [`scripts/run-minio-correctness-gates.sh`](../scripts/run-minio-correctness-gates.sh) — full G2 MinIO correctness (Docker testcontainers).
2. [`scripts/run-large-benchmark-program.sh`](../scripts/run-large-benchmark-program.sh) `--tier l1 --dry-run --skip-g2` — G3→G5 harness preflight (ingest/bench/tpuf/id-overlap/render-report fixtures only).
3. `facts check --tags bench-large` and `facts check --tags bench-tpuf`.

Does **not** run live AWS/tpuf ingest or [`run-minio-large-schema-example.sh`](../scripts/run-minio-large-schema-example.sh) (100k MinIO schema example is operator-maintained; see committed `large-aws-l1-schema-minio.example.json`).

### Phase 4 — Metrics matrix

Collect the same **logical** metrics on both sides where APIs allow. JSON field names align across [`bench-large.sh`](../scripts/bench-large.sh) and [`run_benchmark.py`](../benchmarks/tpuf_driver/run_benchmark.py) for [`render-report.sh`](../scripts/render-report.sh).

| Metric | Unit | openpuffer source | turbopuffer source | Gate / notes |
|--------|------|-------------------|--------------------|--------------|
| Cold p50 query latency | ms | `p50_query_latency_ms` in `large-aws-*.json` | `p50_query_latency_ms` in `tpuf-*.json` | **AWS: p50 < 600** when `OPENPUFFER_BENCH_ENFORCE_GATES=1` |
| Cold p95 query latency | ms | `p95_query_latency_ms` | `p95_query_latency_ms` | Report only (no hard gate) |
| `storage_roundtrips` | count | `performance.storage_roundtrips` (last cold run) | n/a | **≤ 4** on caught-up strong cold vector query |
| `cold_s3_keys_fetched` | count | `performance.cold_s3_keys_fetched` | n/a | Operability; explain vs tpuf opaque storage |
| `s3_get_count` | count | `GET /v1/debug/cache-stats` after cold series | n/a | Segment cache; see `s3_get_count_note` in JSON |
| `candidates_ratio` | ratio | `performance.candidates_ratio` | tpuf `performance` if present | **< 0.20** @ 100k+ (MinIO nightly); informational on AWS |
| `recall@10` | ratio | `POST …/recall` → `avg_recall` | `namespace.recall()` | **≥ 0.85** large-tier script gate; aim **≥ 0.90** @ 100k |
| Index catch-up | bool | `index_cursor_eq_wal_commit_seq` | driver index wait | Must be true before cold series |
| `preferred_ann_version` | int | namespace meta | n/a | **3** required |
| `index_object_count` | count | optional `aws s3api list-objects-v2` | n/a | openpuffer operability |
| Ingest upsert wall time | s | `ingest_elapsed_secs` / `ingest_timing.upsert_wall_sec` in `ingest-large-*.json` | `ingest_elapsed_secs` in `tpuf-*.json` | Not 1:1 vs tpuf (WAL ~1 commit/s/ns) |
| Index wait time | s | `index_wait_sec` (meta poll after upsert) | n/a (tpuf driver includes in ingest wait) | openpuffer operability |
| Ingest docs/s | docs/s | `ingest_docs_per_sec` | `ingest_docs_per_sec` | Upsert wall only |
| Batch upsert p50 | ms | `ingest_timing.batch_latency_ms.p50` | — | Per-batch POST latency |
| Per-run cold detail | JSON array | `cold_runs[]` | `cold_runs[]` | Latency + performance per run |

**Recall billing (tpuf):** use `queries.json` `recall_defaults` (`num=20`, `top_k=10`) — same as openpuffer bench. Lower `num` on L2/L3 if cost-sensitive.

### Phase 4 — Cold query protocol (mandatory)

Shared definition in workload [`queries.json`](../benchmarks/workloads/synthetic-128/l1-100k/queries.json) → `cold_query_protocol`:

| Parameter | Default | Override env |
|-----------|---------|--------------|
| Runs | **7** | `OPENPUFFER_BENCH_COLD_RUNS` / `TURBOPUFFER_BENCH_COLD_RUNS` |
| `top_k` | **10** | from `queries.json` |
| `consistency` | **strong** | from `queries.json` |
| Primary query | `vector-q00` (`vector_queries[0]`) | fixed in scripts |

**Procedure (both systems):**

1. **Indexed gate** — openpuffer: `index_cursor == wal_commit_seq` and `preferred_ann_version == 3` (`bench-large.sh` polls; tpuf driver waits on namespace metadata).
2. **Query shape** — vector-only ANN: `rank_by: ["vector","ANN","embedding", <query_vec>]`, minimal attributes (scripts build body from `queries.json`).
3. **Cache bust each run:**
   - **openpuffer:** empty `--cache-dir` on `serve`; `POST /v1/debug/cache-stats/reset` before each of the 7 queries ([`bench-large.sh`](../scripts/bench-large.sh)).
   - **turbopuffer:** fresh ephemeral namespace per run series (simplest cold path); see [tpuf driver README](../benchmarks/tpuf_driver/README.md).
4. **Execute** 7 cold queries; record client `latency_ms` per run in `cold_runs[]`.
5. **Aggregate** — sort latencies ascending; **p50** = 50th percentile, **p95** = 95th percentile (same formula as [`tests/bench_cold.rs`](../tests/bench_cold.rs)).
6. **Post-series** — one extra cold query for `s3_get_count` (openpuffer); then `/recall` with `recall_defaults`.

**openpuffer (after ingest):**

```bash
export OPENPUFFER_S3_ENDPOINT=... OPENPUFFER_S3_BUCKET=... OPENPUFFER_S3_ACCESS_KEY=... OPENPUFFER_S3_SECRET_KEY=...
export OPENPUFFER_ANN_VERSION=3
export OPENPUFFER_COLD_S3_CONCURRENCY=32   # try 64 if RTT-bound on AWS

./scripts/ingest-large.sh --tier l1
./scripts/bench-large.sh --tier l1
# Record-only if gates not met yet:
OPENPUFFER_BENCH_ENFORCE_GATES=0 ./scripts/bench-large.sh --tier l1
```

**turbopuffer (same tier, same seed):**

```bash
export TURBOPUFFER_API_KEY=...   # never commit
export TURBOPUFFER_REGION=aws-us-east-1   # align with EC2 + S3

python3 benchmarks/tpuf_driver/run_benchmark.py --tier l1
# Output: benchmarks/results/tpuf-l1.json
```

**Warm queries (secondary):** `./scripts/bench-large.sh --warm` (openpuffer) or `./scripts/run-tpuf-large-benchmark.sh --warm` / `run_benchmark.py --warm` (tpuf `hint_cache_warm` + 20× eventual from `warm_query_protocol`). JSON fields `p50_warm_query_latency_ms` / `p95_warm_query_latency_ms`; `render-report.sh` shows warm rows when present.

**Hybrid / filter (secondary):** `bench-large.sh` and `run_benchmark.py` run all `filter_queries` / `hybrid_queries` from `queries.json` (1× each, strong) after cold vector runs; JSON fields `filter_query_runs` / `hybrid_query_runs` (openpuffer hybrid resets cache each). With `--warm`, openpuffer also records `warm_filter_query_runs` / `warm_hybrid_query_runs` @ eventual. G2 integration gates assert correctness on MinIO; `storage_roundtrips ≤ 4` on hybrid.

### Phase 5 — Debugging playbook

When gates fail or latencies look wrong, work in this order ([full detail in plan Phase 5](PLAN_LARGE_DATASET_BENCHMARK.md#phase-5--debugging-playbook)).

#### 5.1 Index not caught up

| Symptom | Check | Fix |
|---------|-------|-----|
| Low recall, high `candidates_ratio`, slow scans | `GET /v1/namespaces/{ns}` → `index_cursor`, `wal_commit_seq`, `unindexed_bytes` | Wait; re-run `ingest-large.sh` poll; check indexer logs |

#### 5.2 Cold path fetching too much

| Symptom | Check | Fix |
|---------|-------|-----|
| `storage_roundtrips > 4`, `cold_s3_keys_fetched` ≫ probed clusters | `performance.ann_probed_clusters`, `OPENPUFFER_ANN_*_PROBE`, `preferred_ann_version` | Re-index with v3; tune probes; see [ANN probe tuning](#ann-probe-tuning-serve--indexer) |

#### 5.3 High latency, low roundtrips

| Symptom | Check | Fix |
|---------|-------|-----|
| p50 > 600 ms but `storage_roundtrips ≤ 4` | S3 RTT from EC2, region mismatch, `OPENPUFFER_COLD_S3_CONCURRENCY` | Same-region bucket + host; try 64; disable rerank for latency A/B |

#### 5.4 Recall collapse

| Check | Fix |
|-------|-----|
| v2 index / wrong seed | `OPENPUFFER_ANN_VERSION=3`; confirm `manifest.json` seed matches tpuf ingest |
| Probes too low | Raise coarse/fine probe; watch `candidates_ratio` |
| Rerank off | `OPENPUFFER_ANN_RERANK=1` for recall A/B (latency cost) |

#### 5.5 Ingest stalls

Expected **~1 WAL commit/s** per namespace — do not compare ingest wall time to tpuf without noting the cap ([1M ingest cadence](#1m-ingest-cadence)). On `503 SlowDown`, backoff and verify IAM.

#### 5.6 turbopuffer-specific

| Issue | Action |
|-------|--------|
| 429 / rate limit | Smaller write concurrency; SDK backoff |
| High cold p50 | Closer `TURBOPUFFER_REGION`; same host as openpuffer bench |
| Recall cost | Reduce `recall_defaults.num` on large namespaces |

#### 5.7 S3 forensics (openpuffer)

```bash
aws s3 cp "s3://${OPENPUFFER_S3_BUCKET}/openpuffer/${NAMESPACE}/meta.json" - | jq .
aws s3 ls "s3://${OPENPUFFER_S3_BUCKET}/openpuffer/${NAMESPACE}/index/" | wc -l
```

Compare with `index_object_count` in `large-aws-*.json`.

### Phase 6 — Pass/fail rubric

#### 6.1 openpuffer gates (by tier)

| Tier | Environment | `storage_roundtrips` | `recall@10` | p50 cold | `candidates_ratio` |
|------|-------------|---------------------|-------------|----------|-------------------|
| 10k | MinIO CI | ≤ 4 | ≥ 0.85 (bench) | informational | lib gates |
| 100k | MinIO nightly | ≤ 4 | ≥ 0.88 (bench) / 0.90 (lib) | informational | < 0.20 |
| 100k | **AWS** (`bench-large.sh` l1) | ≤ 4 | ≥ **0.85** | **< 600 ms** | < 0.20 (target) |
| 1M | AWS (`bench-large.sh` l3 or `bench-1m.sh`) | ≤ 4 | ≥ 0.85 | **< 600 ms** | < 0.20 (target) |

Enforced automatically when `OPENPUFFER_BENCH_ENFORCE_GATES=1` (default in `bench-large.sh` / `bench-1m.sh`).

#### 6.2 openpuffer vs turbopuffer (report interpretation)

| Outcome | Meaning |
|---------|---------|
| openpuffer cold p50 **within ~2×** tpuf @ same tier/region | Competitive for self-hosted; tune concurrency/probes |
| openpuffer cold p50 **> 2×** tpuf | Investigate RTT, probe clamp, indexer lag, rerank — not necessarily incorrect |
| openpuffer recall **<<** tpuf | Expected if ANN simpler than prod SPFresh; tune probes/rerank |
| openpuffer recall **≈** tpuf on synthetic | Strong signal; validate on real embeddings before product claims |
| openpuffer ingest **much slower** | Expected (WAL cap); separate write-path narrative in report |

**Ratio column:** `render-report.sh` computes op/tpuf for p50, recall, ingest when both JSON files exist.

#### 6.3 Block release / block report merge

- MinIO **correctness** regression (G2 + integration suite).
- `storage_roundtrips > 4` on caught-up strong cold vector @ 10k/100k.
- `recall@10` below tier gate on AWS after index catch-up.
- Comparison report missing methodology (region, tier, seed, commit SHA).
- Using **MinIO** latencies in the tpuf comparison table.

#### 6.4 Merge report

```bash
./scripts/render-report.sh --date 2026-06-04
# Requires benchmarks/results/large-aws-l1.json + tpuf-l1.json
# Skeleton only: ./scripts/render-report.sh --dry-run
```

Then update [COMPARISON.md](COMPARISON.md) from measured rows ([plan Phase 7](PLAN_LARGE_DATASET_BENCHMARK.md#phase-7--comparison-report-deliverable)).

---

## Feature: `bench`

```bash
# Build integration + bench tests (needs Docker for MinIO testcontainers)
cargo build --features bench -q

# 10k cold baseline + roundtrip gate (CI; non-ignored)
cargo test -F bench --test bench_cold -- --nocapture

# Regenerate committed baseline artifact
OPENPUFFER_BENCH_WRITE_BASELINE=1 cargo test -F bench bench_cold_10k_baseline --test bench_cold -- --nocapture
```

### Diffable JSON fields (10k / 100k / 1M)

Bench tests and `scripts/bench-1m.sh` print one JSON object per run with these keys (compare across tiers):

| Field | Meaning |
|-------|---------|
| `benchmark` | `cold_10k`, `cold_50k_v3`, `cold_100k`, or `cold_1m` |
| `environment` | `minio-testcontainers`, `in-memory-lib`, or `aws-s3` |
| `namespace_docs` | Indexed document count |
| `storage_roundtrips` | `performance.storage_roundtrips` on a strong cold vector query |
| `s3_get_count` | `GET /v1/debug/cache-stats` → `s3_get_count` after the query |
| `p50_query_latency_ms` | p50 over 7 cold queries (cache reset each run) |
| `candidates_ratio` | ANN candidate pool fraction |
| `recall_at_10` | ANN vs brute (100k bench, 1M script via `/recall`) |
| `cold_s3_keys_fetched` | `performance.cold_s3_keys_fetched` on last cold query |
| `preferred_ann_version` | Namespace meta (`3` required for v0.3 1M) |
| `index_cursor_eq_wal_commit_seq` | `true` when meta `index_cursor == wal_commit_seq` before query |
| `index_object_count` | S3 keys under `index/` matching `clusters-*` or `centroids-l1-*.bin` (MinIO benches; optional on AWS via `aws` CLI) |
| `s3_get_count_note` | Explains segment-cache counter vs `cold_s3_keys_fetched` (10k baseline) |

Committed 10k snapshot: [`benchmarks/results/baseline-10k.json`](../benchmarks/results/baseline-10k.json).

**Post–Phase A 10k (MinIO, probed cold, 2026-06-03):** `storage_roundtrips` 2, `cold_s3_keys_fetched` 15, `p50_query_latency_ms` ~700 (debug CI profile; see [`baseline-10k.json`](../benchmarks/results/baseline-10k.json)), `candidates_ratio` ~0.008, `index_object_count` ~144 (not all index objects fetched on cold query).

Optional nightly artifact: set `OPENPUFFER_BENCH_WRITE_RESULTS=1` on `bench_cold_100k_nightly` → `benchmarks/results/nightly-100k.json`.

### ANN probe tuning (`serve` / indexer)

Set on `openpuffer serve` (and indexer builds) before indexing; values are persisted in `centroids-l0.bin` (`probe_coarse`, `probe_fine`). Rebuild the namespace index after changing probes.

| CLI flag | Environment variable | Default | Effect |
|----------|---------------------|---------|--------|
| `--ann-coarse-probe` | `OPENPUFFER_ANN_COARSE_PROBE` | **4** | Top-*C* L0 coarse centroids probed per query |
| `--ann-fine-probe` | `OPENPUFFER_ANN_FINE_PROBE` | **2** | Top-*F* L1 fine centroids per coarse |

Higher probes → better recall, more `cold_s3_keys_fetched` / `performance.candidates` / `storage_roundtrips`. See [ARCHITECTURE.md](ARCHITECTURE.md#vector-ann-spfresh-inspired) for the query path. Related: `OPENPUFFER_ANN_VERSION` (2/3), `OPENPUFFER_ANN_RERANK` (exact re-rank pool).

**Runtime cluster cap (query path):** even if L0 was built with huge probe env values, `serve` clamps `probe_coarse` / `probe_fine` at query time so cluster `GetObject` count stays ≤ `C + C×F + 4` and ≤ `OPENPUFFER_ANN_MAX_PROBE_CLUSTERS` (default **64**). Emits `tracing` warn + `openpuffer_ann_probe_clamp_total` when clamping.

| Environment variable | Default | Effect |
|---------------------|---------|--------|
| `OPENPUFFER_ANN_MAX_PROBE_CLUSTERS` | **64** | Max cluster segments fetched per probed vector query |

### Cold S3 fetch tuning (`serve` + cold query path)

| Environment variable | Default | Effect |
|---------------------|---------|--------|
| `OPENPUFFER_COLD_MAX_KEYS_PER_ROUND` | **128** | Max keys per logical cold round before splitting into sequential sub-batches (one `storage_roundtrip` per round) |
| `OPENPUFFER_COLD_S3_CONCURRENCY` | **32** | In-flight parallel `GetObject` calls **within** each sub-batch ([`fetch_round`](../src/s3_batch.rs)); `aws-sdk` uses a process-wide shared hyper client ([`shared_s3_http_client`](../src/config.rs)) for connection reuse |

On AWS 1M, try raising concurrency (e.g. `64`) if RTT-bound; lower on memory-constrained MinIO dev boxes.

## Tiers

| Tier | Size | Command | Environment |
|------|------|---------|-------------|
| **CI** | 10k | `cargo test -F bench --test bench_cold` + lib 10k ANN gates | MinIO testcontainers (`.github/workflows/ci.yml` job `bench-10k`) |
| **Mid-tier** | 50k | `cargo test --release -F large_stress --test stress_50k -- --ignored` | MinIO v3 + cold probed (`fifty_thousand_docs_v3_cold_probed_validation`); optional warm v2 stress |
| **Nightly** | 100k | `cargo test -F bench --test bench_cold -- --ignored` + lib `--ignored` | MinIO + in-memory v3 (`.github/workflows/nightly-stress.yml` job `bench-100k`) |
| **Manual** | 1M | [`scripts/bench-1m.sh`](../scripts/bench-1m.sh) | AWS S3 |

### CI (10k gates)

GitHub Actions job **`bench-10k`** runs:

```bash
cargo test --test synthetic_workload_gate -- --nocapture
cargo test -F bench --test bench_cold -- --nocapture
cargo test --lib recall_v3_at_least_five_points_above_v2_on_10k_fixture -- --nocapture
cargo test --lib recall_at_10_10k_with_rerank_at_least_point_nine_two -- --nocapture
cargo test --lib ann_v3_index_object_count_100k_under_five_hundred -- --nocapture
```

Non-ignored bench tests: `bench_cold_10k_baseline`, `bench_cold_10k_warm_vs_cold`, `bench_cold_10k_storage_roundtrips_at_most_four`.

### Nightly (100k + lib ignored)

Scheduled **03:00 UTC** (or `workflow_dispatch`):

```bash
cargo test --release -F bench --test bench_cold -- --ignored --nocapture
cargo test --release --lib \
  recall_at_10_100k_synthetic_at_least_point_nine \
  ann_v3_built_index_object_count_100k_under_five_hundred \
  -- --ignored --nocapture
```

Gates: `recall@10 ≥ 0.88`, `candidates_ratio < 0.20`, `storage_roundtrips ≤ 4` (100k MinIO); lib recall ≥ 0.90, built index objects < 500.

### Mid-tier (50k v3 + cold probed, optional)

Between CI 10k and nightly 100k. Not scheduled in CI by default (`#[ignore]`).

```bash
cargo build --release --features large_stress
# v3 index + strong cold probed path (roundtrips, recall, candidates_ratio, object count)
cargo test --release -F large_stress --test stress_50k \
  fifty_thousand_docs_v3_cold_probed_validation -- --ignored --nocapture

# v2 default warm ANN candidate-ratio stress (same ingest pattern)
cargo test --release -F large_stress --test stress_50k \
  fifty_thousand_docs_indexed_query -- --ignored --nocapture

# Fast wiring when 50k ingest is unavailable (~2k docs, same cold metrics)
cargo test -F large_stress --test stress_50k v3_cold_probed_wiring_at_2k -- --ignored --nocapture
```

**Gates @ 50k** (`fifty_thousand_docs_v3_cold_probed_validation`):

| Metric | Target |
|--------|--------|
| `ann_version` (L0) | `3` (`--ann-version 3` on `serve`) |
| `storage_roundtrips` | ≤ 4 (strong cold, empty `--cache-dir`) |
| `recall_at_10` | ≥ 0.86 (10 synthetic queries vs brute) |
| `candidates_ratio` | < 0.20 |
| `index_object_count` | > 0 and < 500 |
| `cold_s3_keys_fetched` / `ann_probed_clusters` | ≥ 1 |

Prints diffable JSON with `"benchmark": "cold_50k_v3"`. Typical dev machine (**release**): ~45–90s ingest+index + recall (~1–2 min total); use `--release` or indexing may exceed the 300s wall timeout.

**Measured @ 50k (MinIO testcontainers, release, 2026-06-03):** `storage_roundtrips` **2**, `recall_at_10` **1.0**, `index_object_count` **175**, `ann_version` **3** (strong cold, empty `--cache-dir`). Gates also require `candidates_ratio` < 0.20 and `storage_roundtrips` ≤ 4.

## 1M ingest cadence

Operational ingest for the manual AWS gate (from [PLAN risks](PLAN_SPFRESH_AND_COLD_1M.md#risks-and-mitigations): stay under the per-namespace WAL commit rate).

| Step | Setting | Notes |
|------|---------|--------|
| **Batch size** | **10,000** rows per `POST /v2/namespaces/{name}` | Same as 50k stress (`OPENPUFFER_MAX_UPSERT_ROWS`); **100** commits for 1M docs |
| **Commit spacing** | **~1.1s** between batches | Matches README 50k stress; targets **~1 WAL commit/s** (see `OPENPUFFER_WRITE_MAX_DELAY_MS` in [ARCHITECTURE.md](ARCHITECTURE.md#limits)) |
| **ANN build** | `OPENPUFFER_ANN_VERSION=3` on `serve` before/during ingest | Indexer sets `preferred_ann_version == 3` in meta after first v3 index commit |
| **Index catch-up** | Poll `GET /v1/namespaces/{name}` until `index_cursor == wal_commit_seq` | [`scripts/bench-1m.sh`](../scripts/bench-1m.sh): **2s** interval, default **7200s** timeout (`OPENPUFFER_BENCH_INDEX_TIMEOUT_SEC`); also require `preferred_ann_version == 3` before cold queries |
| **Optional** | `block_until_indexed: true` on last batch | Blocks up to **30s** per write; not practical for 1M — use meta polling instead |

Example ingest loop (128-dim `f32`, columnar `upsert_columns`):

```bash
BATCH=10000
for start in $(seq 0 $BATCH 999000); do
  curl -sf -X POST "$BASE/v2/namespaces/$NS" -H 'Content-Type: application/json' \
    -d "$(jq -n --argjson start "$start" '{ upsert_columns: { id: [...], embedding: [...] } }')"
  sleep 1.1
done
# Then poll until index_cursor catches wal_commit_seq (bench-1m.sh does this).
```

At ~1 commit/s, ingest is **~17–20 min**; indexing lag depends on cluster size — plan **1–2 h** wall time before `bench-1m.sh` on a single namespace.

## 1M manual (AWS, v0.3)

**Prerequisites**

1. AWS S3 bucket in the target region; IAM user or role with read/write on the bucket.
2. **Ingest out of band:** follow [1M ingest cadence](#1m-ingest-cadence) above. Index with **`OPENPUFFER_ANN_VERSION=3`** (or `serve --ann-version 3`) so namespace meta has **`preferred_ann_version == 3`** and **`index_cursor == wal_commit_seq`** before benchmarking.
3. Tools on the runner: `bash`, `curl`, `jq`, `python3`, `cargo` (script builds release `openpuffer`). Optional: `aws` CLI for `index_object_count` / `index_keys_total` in the JSON artifact.
4. Do **not** use MinIO timings for the p50 SLO; AWS WAN latency is the gate.

**Dry-run** (no AWS credentials, no `serve`):

```bash
./scripts/bench-1m.sh --dry-run
# or: OPENPUFFER_BENCH_DRY_RUN=1 ./scripts/bench-1m.sh
```

Validates toolchain, defaults `OPENPUFFER_ANN_VERSION=3`, and prints bench tuning. S3 env vars are optional in dry-run.

**Environment variables**

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `OPENPUFFER_S3_ENDPOINT` | yes* | — | AWS S3 endpoint URL (*not in dry-run) |
| `OPENPUFFER_S3_BUCKET` | yes* | — | Bucket name |
| `OPENPUFFER_S3_ACCESS_KEY` | yes* | — | Access key |
| `OPENPUFFER_S3_SECRET_KEY` | yes* | — | Secret key |
| `OPENPUFFER_S3_REGION` | no | `us-east-1` | Region passed to `serve` / `aws` list |
| `OPENPUFFER_ANN_VERSION` | no | **`3`** | Passed to `serve --ann-version` (warn if not 3) |
| `OPENPUFFER_BENCH_DRY_RUN` | no | — | Set `1` or use `--dry-run` |
| `OPENPUFFER_BENCH_NAMESPACE` | no | `bench-1m-cold` | Namespace to benchmark |
| `OPENPUFFER_BENCH_DOCS` | no | `1000000` | Expected doc count (metadata only) |
| `OPENPUFFER_BENCH_LISTEN` | no | `127.0.0.1:8080` | `serve` listen address |
| `OPENPUFFER_BENCH_RESULTS` | no | `benchmarks/results/1m-aws.json` | Output path |
| `OPENPUFFER_BENCH_COLD_RUNS` | no | `7` | Cold query samples for p50 |
| `OPENPUFFER_BENCH_RECALL_NUM` | no | `20` | `/recall` query count |
| `OPENPUFFER_BENCH_INDEX_TIMEOUT_SEC` | no | `7200` | Wait for indexer catch-up |
| `OPENPUFFER_BENCH_SKIP_SERVE` | no | — | Set if `serve` already running |
| `OPENPUFFER_BENCH_SKIP_INDEX_WAIT` | no | — | Still verifies meta; skips poll loop |
| `OPENPUFFER_BENCH_SKIP_INDEX_STATS` | no | — | Skip optional `aws s3api list-objects-v2` |
| `OPENPUFFER_BENCH_ENFORCE_GATES` | no | `1` | Exit 1 if SLOs fail |

**Run**

```bash
export OPENPUFFER_S3_ENDPOINT=...
export OPENPUFFER_S3_BUCKET=...
export OPENPUFFER_S3_ACCESS_KEY=...
export OPENPUFFER_S3_SECRET_KEY=...
export OPENPUFFER_ANN_VERSION=3   # default in script; required for v0.3 meta gate

# After ingest + index catch-up (preferred_ann_version==3, index_cursor==wal_commit_seq):
./scripts/bench-1m.sh
# or record without failing on SLO:
OPENPUFFER_BENCH_ENFORCE_GATES=0 ./scripts/bench-1m.sh
```

Output JSON matches **10k / 100k** tiers (`cold_s3_keys_fetched`, `s3_get_count`, `s3_get_count_note`, `index_cursor_eq_wal_commit_seq`, `preferred_ann_version`, plus `recall_at_10` and optional `index_object_count`).

**Targets** (written to `benchmarks/results/1m-aws.json`):

- `preferred_ann_version == 3` and `index_cursor_eq_wal_commit_seq == true` (checked before cold queries)
- `storage_roundtrips ≤ 4`
- `recall_at_10 ≥ 0.85` (from `POST /v1/namespaces/{name}/recall`)
- `p50_query_latency_ms < 600` on AWS

## Phase B ANN gates (lib, CI + nightly)

| Gate | CI command | Nightly (full 100k build) |
|------|------------|---------------------------|
| v3 object count @ 100k | `cargo test --lib ann_v3_index_object_count_100k_under_five_hundred` | `cargo test --lib ann_v3_built_index_object_count_100k_under_five_hundred -- --ignored` |
| v3 vs v2 @ 10k (+0.05) | `cargo test --lib recall_v3_at_least_five_points_above_v2_on_10k_fixture` | — |
| recall@10 @ 100k ≥ 0.90 | sizing + spot-check in CI | `cargo test --lib recall_at_10_100k_synthetic_at_least_point_nine -- --ignored` |

Build v3 indexes with `OPENPUFFER_ANN_VERSION=3` on `serve` / indexer; lib tests set `AnnBuildConfig::with_ann_version(3)` directly.

## Related tests

- `cargo test -F perf` — 5k in-memory `candidates_ratio < 0.12`
- `cargo test -F integration recall_http_response_shape_on_minio recall_http_with_filters` — `/recall` shape + filters; uses `queries.json` `recall_defaults` (num=20, top_k=10)
- `cargo test -F integration ten_thousand_docs_indexed_query` — 10k indexed ANN smoke (warm path)
- `cargo test -F integration s3_cold_query_reports_roundtrips_on_minio` — small-namespace cold roundtrips
- `cargo test --release -F large_stress --test stress_50k -- --ignored` — 50k warm (v2) + v3 cold probed mid-tier

## Facts

```bash
facts check --tags ann                 # Phase B @spec gates (7 facts, includes ignored 100k recall)
facts check --tags "ann or cold"       # Phase A+B program gates (10 spec facts)
facts check --tags bench-large         # large-dataset harness A1–A3, G2 fixture gates (PLAN_LARGE_DATASET_BENCHMARK)
facts check --tags bench-tpuf          # turbopuffer driver A4–A5, comparison report merge
facts ll --tags spec          # list program spec facts
```

Large-tier comparison program ([`PLAN_LARGE_DATASET_BENCHMARK.md`](PLAN_LARGE_DATASET_BENCHMARK.md)): `@spec` facts under tags `bench-large` and `bench-tpuf` cover `generate_synthetic.py`, `ingest-large.sh`, `bench-large.sh`, `run-minio-correctness-gates.sh`, `tpuf_driver/run_benchmark.py`, and `render-report.sh`. Operator procedures: [§ Phases 4–6 runbook](#large-dataset-program--operator-runbook-phases-46). Live `benchmarks/results/large-aws-*.json` / `tpuf-*.json` on AWS remain manual until operators run ingest + bench.