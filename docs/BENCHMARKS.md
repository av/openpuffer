# Benchmarks

Measurable baselines and scale gates for [PLAN_SPFRESH_AND_COLD_1M.md](PLAN_SPFRESH_AND_COLD_1M.md). Work is fact-driven: `@spec` facts under `index/ann` and `query/cold` in `.facts` are checked with `facts check --tags cold,ann`.

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
- `cargo test -F integration ten_thousand_docs_indexed_query` — 10k indexed ANN smoke (warm path)
- `cargo test -F integration s3_cold_query_reports_roundtrips_on_minio` — small-namespace cold roundtrips
- `cargo test --release -F large_stress --test stress_50k -- --ignored` — 50k warm (v2) + v3 cold probed mid-tier

## Facts

```bash
facts check --tags ann                 # Phase B @spec gates (7 facts, includes ignored 100k recall)
facts check --tags "ann or cold"       # Phase A+B program gates (10 spec facts)
facts ll --tags spec          # list program spec facts
```