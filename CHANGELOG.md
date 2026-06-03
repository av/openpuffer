# Changelog

All notable changes to openpuffer are documented here. Versioning follows [SemVer](https://semver.org/).

## [0.2.0] — 2026-06-03

Major release: **turbopuffer-aligned storage architecture** (WAL + async indexes on S3). The HTTP API remains turbopuffer-compatible for core write/query paths; the **durable layout is new** and not compatible with v0.1.x per-document JSON storage.

### Breaking

- **Removed** per-document `docs/{id}.json` and `manifest.json` as the system of record.
- Durable state is **`meta.json` + `wal/{seq}.bin` + `index/*`** under `openpuffer/{namespace}/` only.
- Namespaces without `meta.json` are treated as **empty** (no automatic migration from v0.1 layout).

### Storage & write path

- Group-commit write buffer (time / batch size) → one WAL PUT + `meta.json` CAS per batch; HTTP ACK after durable WAL commit (**strong consistency**).
- WAL v1 wire format: `[0x01][bincode WalEntry][crc32 LE]`; legacy bincode segments still replay; `OPENPUFFER_WAL_CORRUPT_POLICY` (`fail` | `skip`).
- WAL compaction: snapshot + delete old segments when fully indexed.
- Per-namespace commit lock + meta CAS retries; ~**1 WAL commit/s/namespace** by default (throughput cap, not production turbopuffer scale).

### Indexing (async background)

- **FTS**: BM25 inverted segments on S3; Unicode NFKC tokenizer, stopwords, optional Porter stem (`OPENPUFFER_FTS_STEM`).
- **Vector ANN**: simplified two-level k-means (L0/L1 centroids + clusters), k-means++ init, configurable coarse/fine probes; `cosine_distance` / `euclidean_squared`; `[N]f32` and `[N]f16` schema.
- **Filters**: attribute filter index segments; intersect before scoring.
- Incremental segment merges; fair multi-namespace indexer round-robin.
- `index_cursor` in meta tracks merge progress; queries scan **unindexed WAL tail** under strong consistency.

### Query

- Hybrid `rank_by` (`Sum` / `Product`), BM25, ANN, attribute filters (`Eq`, `In`, `And`, …).
- `consistency: "strong"` (default) vs `"eventual"` (skips WAL tail on pinned views for lower latency).
- `order_by` tie-break after ranking; `performance` block (candidates, ratio, billing estimates).
- Optional `--cache-dir` disk mirror + `POST /v1/namespaces/{name}/warm` view pin; batched S3 reads (`s3_batch`) on cold path.

### Write / namespace API (subset)

- `schema` on write (`uuid`, `[]uuid`, `datetime`, vectors, filterable / full_text_search hints).
- `upsert_condition`, `delete_by_filter`, `patch_by_filter`, `patch_rows` / `patch_columns`.
- `copy_from_namespace`, `branch_from_namespace`.
- `distance_metric`, `return_affected_ids`, `include_vectors` / `vector_encoding` (float | base64).
- Namespace export, deep health S3 probe, limits enforcement (namespace name, batch sizes, 64 MiB body).

### Operations & testing

- Docker Compose dev stack (MinIO) and external S3 integration harness.
- Integration tests assert real S3 objects (MinIO testcontainers or `OPENPUFFER_TEST_S3_*`); 40+ integration scenarios including 10k-doc stress, compaction, multi-instance, S3 byte proofs.
- Prometheus `/metrics`; consistent `{"error","status"}` JSON errors.

### Honest limitations (unchanged vs turbopuffer prod)

- Single binary, no managed cloud, no cross-region replication.
- ANN is **simplified k-means probing**, not SPFresh or turbopuffer’s production ANN stack.
- Throughput and merge semantics are v1 simplifications; not validated at turbopuffer scale.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and [docs/COMPARISON.md](docs/COMPARISON.md) for design detail and gap list.

## [0.1.0] — earlier

Initial release: turbopuffer-shaped HTTP API with per-document JSON objects on S3 (`docs/{id}.json`, manifest). Superseded by 0.2.0 storage layout.