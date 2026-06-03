# openpuffer

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

WAL replay verifies CRC on v1 segments; corrupt segments abort by default (`OPENPUFFER_WAL_CORRUPT_POLICY=fail`, or `skip` to continue after prior segments). Legacy segments without the `0x01` prefix remain readable.

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

Smoke test:

```bash
curl -s http://127.0.0.1:8080/health
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
| `--cache-dir`, `OPENPUFFER_CACHE_DIR` | Index segment disk cache (default `/tmp/openpuffer-cache`; `""` = memory-only) |
| `OPENPUFFER_WRITE_MAX_DELAY_MS` | Group-commit delay (default 1000) |
| `OPENPUFFER_WRITE_MAX_BATCH_OPS` | Max ops per WAL batch (default 512) |
| `OPENPUFFER_MAX_PINNED_NAMESPACES` | In-process warm view LRU (default 32) |
| `OPENPUFFER_WAL_CORRUPT_POLICY` | WAL replay on corrupt segment: `fail` (default) or `skip` |
| `OPENPUFFER_ANN_COARSE_PROBE` / `OPENPUFFER_ANN_FINE_PROBE` | ANN L0/L1 cluster probe counts (defaults 4 / 2) |

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
| GET | `/health` | Readiness |
| GET | `/v1/namespaces` | List namespaces + metadata |
| GET | `/health?deep=1` | S3 probe (`HeadBucket` + `openpuffer/` read); `degraded` if down |
| GET | `/v1/namespaces/{name}` | `approx_row_count`, `index_status`, `unindexed_bytes`, cursors |
| GET/POST | `/v1/namespaces/{name}/export` | Paginated export (`last_id`, `limit`, `format=ndjson`) |
| POST | `/v1/namespaces/{name}/warm` | Prefetch index + pin view |
| POST | `/v2/namespaces/{name}` | Write (upsert, patch, delete, `delete_by_filter`, `patch_by_filter`, `schema`) |
| POST | `/v2/namespaces/{name}/query` | Vector / FTS / hybrid / filtered search |
| DELETE | `/v2/namespaces/{name}` | Delete namespace prefix |

Query responses include `performance` (`candidates`, `candidates_ratio`, `exhaustive_search_count`, …) and optional headers `X-Openpuffer-Candidates`, `X-Openpuffer-Candidates-Fraction`.

## Test

```bash
cargo test                              # unit tests (no Docker)
cargo build --features integration      # build binary for integration tests
cargo test -F integration               # MinIO testcontainers (WAL, index, restart, warm, export, …)
cargo test -F perf                      # 5k-doc ANN candidate_ratio regression
```

**S3 integration (requires Docker):**

```bash
./scripts/run-integration-s3.sh
```

This builds the server binary and runs `cargo test -F integration` against **real MinIO** via testcontainers. Integration tests assert **Head/List/Get** on `meta.json`, `wal/`, and `index/` (decode WAL, segment growth, copy key parity) — not HTTP-only mocks.

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