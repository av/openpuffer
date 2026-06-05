# openpuffer

[![CI](https://github.com/av/openpuffer/actions/workflows/ci.yml/badge.svg)](https://github.com/av/openpuffer/actions/workflows/ci.yml)
[![version](https://img.shields.io/badge/version-0.2.0-blue)](CHANGELOG.md)

Stateless vector and full-text search server backed by **S3-compatible object storage**. HTTP API is compatible with [turbopuffer](https://turbopuffer.com/docs) core write/query paths; the **on-disk architecture** follows [turbopuffer’s WAL + index model](https://turbopuffer.com/docs/architecture), not a per-document JSON store.

## How it compares to turbopuffer

| Area | turbopuffer | openpuffer (v1) |
|------|-------------|-----------------|
| **Durable layout** | WAL + index segments on object storage | Same: `meta.json`, `wal/{seq}.bin`, `index/*` under `openpuffer/{ns}/` |
| **Write ACK** | After durable WAL commit | Group-commit buffer → one WAL PUT + `meta.json` CAS per batch |
| **Indexing** | Async SPFresh-style ANN + FTS | Async background indexer: BM25 FTS, k-means centroids/clusters, attribute filter index |
| **Query** | Indexed candidates + unindexed WAL tail | `strong` (default); `eventual` skips WAL tail + catch-up on pinned views (sub-10ms warm path) |
| **Cache** | NVMe + in-process warm | Optional `--cache-dir` disk mirror + `POST …/warm` view pin |
| **Scale / polish** | Production multi-tenant | Single binary, MinIO integration tests; simplified ANN (one-level k-means, no SPFresh hierarchy) |
| **API surface** | Full product API | Core write/query/metadata/export/warm; no billing portal, CMEK, or all v2 edge cases |

**Honest gaps:** no managed cloud, no cross-region replication, ANN is a simplified two-level k-means probe (not production SPFresh), throughput is ~1 WAL commit/s/namespace by default, and filter/FTS merges are simpler than turbopuffer at scale.

**Full comparison (implemented vs missing, when to use which):** [docs/COMPARISON.md](docs/COMPARISON.md).

Design detail: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Architecture (high level)

```
                    ┌─────────────┐
  POST /v2/...      │ write       │  group-commit buffer (time / batch size)
  (upsert/delete)   │ buffer      │
                    └──────┬──────┘
                           │ durable ACK
                           ▼
              S3: openpuffer/{ns}/
              ├── meta.json          ← index_cursor, wal_commit_seq, schema
              ├── wal/
              │   ├── 00000001.bin   ← [0x01][bincode WalEntry][crc32 LE]
              │   └── snapshot.bin   ← compaction snapshot (optional)
              └── index/             ← async indexer
                  ├── fts-*.bin
                  ├── {field}/centroids-l0.bin + centroids-l1-*.bin + clusters-*.bin
                  └── filter-*.bin

  POST /v2/.../query
       │
       ├─ load meta + index segments (disk cache if warm)
       ├─ ANN (L0/L1 probe) / BM25 / hybrid candidate generation
       ├─ apply filters (intersect before score)
       └─ score unindexed WAL tail (strong) → top_k
```

WAL replay verifies CRC on v1 segments; corrupt segments use [`fail` or `skip`](#wal-corrupt-policy) (default `fail`). Legacy segments without the `0x01` prefix remain readable.

**Consistency:** writes are visible after `wal_commit_seq` advances; queries under `consistency: "strong"` also scan WAL entries with `seq > index_cursor` until the indexer catches up.

## Features

- WAL-backed writes with strong consistency before ACK
- Background indexer (FTS BM25, vector ANN clusters, attribute filters)
- Vector ANN, BM25 FTS, hybrid `rank_by` (`Sum` / `Product`)
- Query filters (`Eq`, `In`, `And`, …), `delete_by_filter`, `patch_by_filter`, `patch_rows`
- Namespace export at `wal_commit_seq`, warm-cache endpoint
- Single static binary — no sidecar databases

## Quickstart (local dev with Docker)

Requires [Docker](https://docs.docker.com/get-docker/) for MinIO.

```bash
./scripts/dev-up.sh      # MinIO on :9000, bucket openpuffer-dev
./scripts/dev-serve.sh   # build + serve on :8080
```

**SPFresh v3 index + cold S3 path** (probed cluster fetch, no disk cache):

```bash
export OPENPUFFER_ANN_VERSION=3          # v3 index layout at build (default 2)
export OPENPUFFER_CACHE_DIR=""           # cold query: S3-only index load (same as --cache-dir "")
export OPENPUFFER_COLD_S3_CONCURRENCY=32 # parallel GETs per cold sub-batch (default 32)
# optional: OPENPUFFER_ANN_RERANK=1      # exact re-rank over probed clusters (higher recall, larger candidate pool)

./scripts/dev-serve.sh
```

After upserts and `index_cursor == wal_commit_seq`, vector queries report `performance.storage_roundtrips`, `cold_s3_keys_fetched`, and `ann_probed_clusters`. See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for probe/cold tuning.

**Benchmarks / performance (MinIO vs turbopuffer scaling):** [benchmarks/OP_VS_TPUF.md](benchmarks/OP_VS_TPUF.md) (one-page verdict); full report — [docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md](docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md); reproduce with `make bench-compare-tpuf` (offline gate: `./scripts/verify-op-scaling-comparison.sh`).

### Large-dataset comparison harness

Apples-to-apples **openpuffer vs turbopuffer** on a shared synthetic workload (L1 default: **100k × 128-dim**). Workloads, result JSON, and operator scripts: [benchmarks/README.md](benchmarks/README.md). Program plan: [docs/PLAN_LARGE_DATASET_BENCHMARK.md](docs/PLAN_LARGE_DATASET_BENCHMARK.md).

**Operator handoff (offline harness complete, live G3–G5 pending):** [docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md](docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md).

**Milestone tag** `large-dataset-harness-v1` (`c7f66a3`, annotated 2026-06-04) — offline harness only; measured `large-aws-l1.json` / `tpuf-l1.json` still require EC2 + AWS S3 + `TURBOPUFFER_API_KEY`:

```bash
git fetch --tags
git checkout large-dataset-harness-v1   # or: git show large-dataset-harness-v1
```

**Makefile targets** (same as `./scripts/verify-large-benchmark-program.sh` and operator preflights):

| Target | Purpose |
|--------|---------|
| `make bench-verify` | Offline harness gate — pytest, schemas, L1–L3 dry-runs, `@spec` facts (CI dispatch) |
| `make bench-dry-run` | Harness dry-run only — no pytest/cargo/facts; no cloud spend |
| `make bench-g2-minio` | Optional G2 MinIO correctness gates (Docker; slow) |
| `make bench-preflight` | G3+G4+overlap preflights (offline default) |

```bash
make bench-verify                                    # before any cloud spend
make bench-verify VERIFY_FLAGS="--with-g2"           # + MinIO G2 (Docker parity with CI)
make bench-dry-run                                   # L1–L3 script dry-runs only
make bench-g2-minio                                  # G2 only (faster iteration)
make bench-preflight                                 # offline cost/deps/overlap checks
make bench-preflight PREFLIGHT_FLAGS="--live --tier l1"   # EC2 live preflight
```

EC2 live runs, artifact `git add -f` policy, and post-live `@spec` activation: [benchmarks/README.md](benchmarks/README.md), [benchmarks/OPERATOR_RUNBOOK_QUICK.md](benchmarks/OPERATOR_RUNBOOK_QUICK.md).

**Recall API** (ANN vs exhaustive on indexed namespace):

```bash
curl -s -X POST "http://127.0.0.1:8080/v1/namespaces/my-ns/recall" \
  -H 'Content-Type: application/json' \
  -d '{"num": 5, "top_k": 10, "vector_field": "embedding"}'
# → {"avg_recall":0.9,"avg_ann_count":...,"avg_exhaustive_count":...}
```

Smoke test:

```bash
curl -s http://127.0.0.1:8080/health
curl -s http://127.0.0.1:8080/v1/ready
curl -s "http://127.0.0.1:8080/health?deep=1"
```

MinIO console: http://127.0.0.1:9001 (`minioadmin` / `minioadmin`). Stop storage with `docker compose down` from the repo root.

## Build

```bash
cargo build --release
```

## Run

Create your bucket first (or use `./scripts/dev-up.sh`), then:

```bash
openpuffer serve \
  --listen 0.0.0.0:8080 \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-bucket openpuffer-dev \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin
```

### Configuration

| Flag / env | Purpose |
|------------|---------|
| `--s3-endpoint`, `OPENPUFFER_S3_ENDPOINT` | S3 API URL |
| `--s3-bucket`, `OPENPUFFER_S3_BUCKET` | Bucket name |
| `--s3-region`, `OPENPUFFER_S3_REGION` | Region (default `us-east-1`) |
| `--s3-access-key` / `--s3-secret-key` | Credentials |
| `--cache-dir`, `OPENPUFFER_CACHE_DIR` | Index segment disk cache (default `/tmp/openpuffer-cache`; `""` = memory-only / **cold S3 path**) |
| `OPENPUFFER_COLD_MAX_KEYS_PER_ROUND` | Max S3 keys per cold round sub-batch (default 128) |
| `OPENPUFFER_COLD_S3_CONCURRENCY` | In-flight parallel `GetObject` per cold sub-batch (default 32) |
| `OPENPUFFER_WRITE_MAX_DELAY_MS` | Group-commit delay (default 1000) |
| `OPENPUFFER_WRITE_MAX_BATCH_OPS` | Max ops per WAL batch (default 512) |
| `OPENPUFFER_MAX_PINNED_NAMESPACES` | In-process warm view LRU (default 32) |
| `--wal-corrupt-policy`, `OPENPUFFER_WAL_CORRUPT_POLICY` | WAL replay on CRC mismatch: `fail` (default) or `skip` — see [WAL corrupt policy](#wal-corrupt-policy) |
| `--ann-version`, `OPENPUFFER_ANN_VERSION` | Index format: `2` (default) or `3` (SPFresh routing + L2 splits) |
| `--ann-coarse-probe` / `--ann-fine-probe`, `OPENPUFFER_ANN_COARSE_PROBE` / `OPENPUFFER_ANN_FINE_PROBE` | ANN L0/L1 probe counts at index build (defaults 4 / 2) |
| `--ann-rerank`, `OPENPUFFER_ANN_RERANK` | Exact re-rank over probed ANN pool (`1`/`true`; default off) |

### Prometheus metrics

Build with `--features metrics` (`cargo build --release --features metrics`). The server exposes **`GET /metrics`** (Prometheus text).

Cold/ANN counters (increment on probed cold loads and vector ANN queries):

| Metric | Meaning |
|--------|---------|
| `openpuffer_cold_s3_keys_fetched` | S3 object keys fetched on cold batch plans (each parallel sub-batch key counts once) |
| `openpuffer_ann_probed_clusters` | Cluster segments selected by ANN probe planning per vector query |
| `openpuffer_ann_probe_clamp_total` | Times on-disk probe widths were clamped to `OPENPUFFER_ANN_MAX_PROBE_CLUSTERS` at query time |

Also exported: `openpuffer_wal_commits_total`, `openpuffer_index_lag_segments`, `openpuffer_s3_get_total`, `openpuffer_query_duration_seconds`, `openpuffer_cold_query_duration_seconds`.

Per-query JSON (`POST …/query`) reports the same cold signals as `performance.cold_s3_keys_fetched` and `performance.ann_probed_clusters` without the metrics feature.

### Debug endpoints (integration builds)

Built with `--features integration` (default for `./scripts/run-integration-s3.sh`). Not for production exposure.

| Method | Path | Body | Purpose |
|--------|------|------|---------|
| GET | `/v1/debug/cache-stats` | — | `s3_get_count` from segment cache |
| POST | `/v1/debug/cache-stats/reset` | — | Reset `s3_get_count` |
| POST | `/v1/debug/namespaces/{name}/cold-plan` | Same JSON as query (`rank_by`, optional `consistency`) | Preview [`plan_cold_query`](src/s3_batch.rs): per-round key counts, `storage_roundtrips` estimate, per-field probe plan — **does not run the query** (fetches `meta.json` and L0 only when vector probes are present) |

Example (cold cache, indexed namespace):

```bash
curl -sS -X POST "http://127.0.0.1:8080/v1/debug/namespaces/my-ns/cold-plan" \
  -H 'Content-Type: application/json' \
  -d '{"rank_by":["vector","ANN","embedding",[1,0,0]],"consistency":"eventual"}' | jq .
```

### WAL corrupt policy

v1 WAL segments on S3 use `[0x01][bincode WalEntry][crc32 LE]`. On replay, openpuffer verifies the CRC over the payload. If a segment is truncated, tampered, or has a bad checksum:

| Policy | Flag / env | Behavior |
|--------|------------|----------|
| **`fail`** (default) | `--wal-corrupt-policy fail` or `OPENPUFFER_WAL_CORRUPT_POLICY=fail` | Namespace load aborts; queries return **500** with a turbopuffer-style `{"error":"…","status":"error"}` mentioning corrupt WAL |
| **`skip`** | `--wal-corrupt-policy skip` or `OPENPUFFER_WAL_CORRUPT_POLICY=skip` | Log the corrupt segment and **continue** replay; earlier segments stay applied; the corrupt segment’s writes are invisible |

Set the policy at **process start** (`openpuffer serve`). Legacy WAL blobs without the `0x01` version byte still replay (no CRC on those segments).

**Example (recovery after partial upload):**

```bash
openpuffer serve \
  --wal-corrupt-policy skip \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-bucket openpuffer-dev \
  ...
```

Integration coverage: `corrupt_wal_segment_on_minio_fail_and_skip_policies` in `tests/integration_s3.rs` (flips one CRC byte on S3, asserts fail → 500 and skip → doc from seq 1 only).

### Operations guide

1. **Cold start** — point at bucket; first write creates `meta.json` + `wal/00000001.bin`.
2. **Indexing lag** — check `GET /v1/namespaces/{name}`: `index_cursor` should reach `wal_commit_seq`. Queries still return recent writes via WAL tail under strong consistency.
3. **Warm a hot namespace** — `POST /v1/namespaces/{name}/warm` prefetches index objects and pins an in-memory view (fewer S3 round-trips on the same process).
4. **Export** — `GET /v1/namespaces/{name}/export?limit=10000&last_id=…` (or POST with JSON body) for a consistent snapshot at `wal_commit_seq`.
5. **Multi-instance** — any number of stateless `serve` processes can share one bucket; per-namespace writes serialize via S3 CAS + in-process commit lock.
6. **Restart** — no local durable state required; replay WAL from S3 on first query.

## API

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Liveness (always `ok` unless `?deep=1`) |
| GET | `/v1/ready` | Traffic readiness — S3 configured and reachable (`503` if not) |
| GET | `/metrics` | Prometheus scrape (`--features metrics`) |
| GET | `/v1/namespaces` | List namespaces + metadata |
| GET | `/health?deep=1` | S3 probe (`HeadBucket` + `openpuffer/` read); `degraded` if down |
| GET | `/v1/namespaces/{name}` | `approx_row_count`, `index_status`, `unindexed_bytes`, cursors |
| GET/POST | `/v1/namespaces/{name}/export` | Paginated export (`last_id`, `limit`, `format=ndjson`) |
| POST | `/v1/namespaces/{name}/warm` | Prefetch index + pin view |
| POST | `/v1/namespaces/{name}/recall` | ANN vs exhaustive recall@k (`num`, `top_k`, optional `filters`, `vector_field`); response `avg_recall`, `avg_ann_count`, `avg_exhaustive_count` |
| POST | `/v2/namespaces/{name}` | Write (upsert, patch, delete, `delete_by_filter`, `patch_by_filter`, `schema`) |
| POST | `/v2/namespaces/{name}/query` | Vector / FTS / hybrid / filtered search |
| DELETE | `/v2/namespaces/{name}` | Delete namespace prefix |

Query responses include `performance` (`candidates`, `candidates_ratio`, `exhaustive_search_count`, …) and optional headers `X-Openpuffer-Candidates`, `X-Openpuffer-Candidates-Fraction`.

## Test

### Test matrix

| Suite | Command | Count | Docker | Notes |
|-------|---------|-------|--------|-------|
| **Unit** | `cargo test` | ~158 | No | Library + WAL/index logic |
| **Integration (MinIO)** | `cargo test -F integration` | **51** | Yes | `tests/integration_s3.rs` — testcontainers MinIO; S3 Head/List/Get + WAL decode |
| **Perf** | `cargo test -F perf` | 1 | Yes | 5k-doc ANN `candidates_ratio` regression |
| **External S3** | `cargo test -F integration --test integration_external_s3 -- --ignored` | 1 | Optional | Compose MinIO or `OPENPUFFER_TEST_S3_*` |
| **Large stress** | `cargo test --release -F large_stress --test stress_50k -- --ignored` | 3 | Yes | 50k warm + v3 cold probed mid-tier; not in default CI |

Typical dev run (unit + integration + perf):

```bash
cargo test                              # unit (~158), no Docker
cargo build --features integration      # build server binary for integration harness
cargo test -F integration               # 51 MinIO scenarios (~60–70s)
cargo test -F perf                      # ANN candidate_ratio on 5k docs
```

**S3 integration (requires Docker) — recommended:**

```bash
./scripts/run-integration-s3.sh
```

Builds with `--features integration` and runs all **51** `integration_s3` tests against **real MinIO** (testcontainers). Tests assert **Head/List/Get** on `meta.json`, `wal/`, and `index/` (decode WAL, segment growth, copy key parity) — not HTTP-only mocks.

### Optional 50k namespace stress (`large_stress`)

Not part of the default matrix — `#[ignore]` so `cargo test -F integration` stays fast. **Nightly CI:** [`.github/workflows/nightly-stress.yml`](.github/workflows/nightly-stress.yml) (03:00 UTC + manual `workflow_dispatch`).

```bash
cargo build --release --features large_stress
cargo test --release -F large_stress --test stress_50k -- --ignored --nocapture
```

Upserts **50k** docs in **5×10k** `upsert_columns` batches with **~1.1s** spacing (WAL rate limit), waits for `index_cursor == wal_commit_seq` (300s wall timeout). Tests:

- `fifty_thousand_docs_indexed_query` — v2 default, warm ANN `candidates_ratio < 0.2`
- `fifty_thousand_docs_v3_cold_probed_validation` — `--ann-version 3`, strong cold probed path: `storage_roundtrips ≤ 4`, `recall@10 ≥ 0.86`, `candidates_ratio < 0.2` (see [docs/BENCHMARKS.md](docs/BENCHMARKS.md) mid-tier)
- `v3_cold_probed_wiring_at_2k` — fast 2k wiring for the same cold metrics

**Use `--release`** — debug builds may not index 50k within 300s. On a typical dev machine (release): warm stress **~40–45s**; v3 cold gate **~1–2 min** including recall probes.

### Testing against real S3

Two ways to hit a **real** S3-compatible endpoint (MinIO or AWS) — not mocks:

| Mode | Command | Backend |
|------|---------|---------|
| **Default** | `./scripts/run-integration-s3.sh` | Ephemeral MinIO (testcontainers) |
| **Compose MinIO** | `./scripts/run-integration-s3.sh external` | `docker-compose.test.yml` on `:9000` |
| **Your bucket** | Set `OPENPUFFER_TEST_S3_*` env vars | Any S3-compatible API |

**Compose external tests** (starts MinIO if `:9000` is not already healthy, creates `openpuffer-integration` bucket):

```bash
./scripts/run-integration-s3.sh external
```

**Manual env** (same variables the script sets; use for CI or a shared MinIO/AWS bucket):

```bash
export OPENPUFFER_TEST_S3_ENDPOINT=http://127.0.0.1:9000
export OPENPUFFER_TEST_S3_BUCKET=openpuffer-integration
export OPENPUFFER_TEST_S3_ACCESS_KEY=minioadmin
export OPENPUFFER_TEST_S3_SECRET_KEY=minioadmin

cargo test -F integration --test integration_external_s3 -- --ignored
```

**Serve against the same bucket** (after `external` or with your own endpoint):

```bash
export OPENPUFFER_S3_ENDPOINT=http://127.0.0.1:9000
export OPENPUFFER_S3_BUCKET=openpuffer-integration
export OPENPUFFER_S3_ACCESS_KEY=minioadmin
export OPENPUFFER_S3_SECRET_KEY=minioadmin
./scripts/dev-serve.sh
```

Stop the test compose stack: `docker compose -f docker-compose.test.yml down`.

### What integration tests assert on S3

- **Head/List/Get** on `meta.json`, `wal/{seq:08}.bin`, and `index/*` (not HTTP-only)
- **Decode** bincode `WalEntry` from `wal/*.bin` and compare doc ids to HTTP export
- **Index layout**: `fts-*.bin`, `filter-*.bin`, `centroids-l0.bin`, `centroids-l1-*.bin` (non-zero size)
- **Incremental growth**: FTS/filter segment sizes or `fts_segment_ids` / `filter_segment_ids` chains grow after a second WAL batch (`s3_fts_and_filter_segments_grow_on_minio`)
- **Copy parity**: `copy_from_namespace` duplicates every source key under the dest prefix (`s3_copy_from_namespace_duplicates_all_keys`)
- **No** legacy `docs/{id}.json` or `manifest.json`

## License

MIT OR Apache-2.0