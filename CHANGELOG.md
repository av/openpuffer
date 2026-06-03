# Changelog

All notable changes to openpuffer are documented here. Versioning follows [SemVer](https://semver.org/).

## [0.3.0] — 2026-06-03

Program release: **SPFresh-inspired ANN (opt-in v3)**, **query-driven cold load @ scale**, **recall API**, and **operational bench/metrics**. Built on the 0.2.0 WAL + S3 index layout.

**Default on-disk ANN remains v2** — index segments written without `OPENPUFFER_ANN_VERSION=3` / `--ann-version 3` use the two-level k-means layout from 0.2.0. v3 routing, L2 splits, and incremental maintenance apply only to namespaces indexed under v3; queries **dual-read** legacy v2 segments.

### Cold

- **Query-driven S3 load** — `fetch_cold_vector_probed`, `ColdBootstrapArtifacts`, `plan_cold_query`; probed L1 + cluster GETs only (not full `num_fine_total` index on cold path).
- **Roundtrip budget** — strong cold vector queries account **`storage_roundtrips ≤ 4`** (meta/L0 → probed L1 → probed clusters → optional WAL tail); caught-up 10k MinIO often **2** roundtrips.
- **Warm path alignment** — indexer/query hot path loads L0 + probed L1/clusters (same planner as cold); full index load reserved for WAL merge and recall evaluation.
- **v3 on probed path** — L2 centroids + `centroids-routing.bin` wired into probed cold/warm load and `query_ann` when `ann_version == 3`.
- **WAL tail (round 4)** — `fetch_cold_unindexed_wal_tail` for strong consistency after probed index fetch.
- **Sub-batching** — `OPENPUFFER_COLD_MAX_KEYS_PER_ROUND` caps keys per logical round inside one roundtrip.
- **S3 concurrency** — `OPENPUFFER_COLD_S3_CONCURRENCY` + shared HTTP client for parallel GETs within a round.
- **Resilience** — missing/empty cluster keys tolerated via `fetch_round_optional`; hybrid **Sum** rank no longer drops WAL-tail docs when min-max normalizes both signals to zero.

### ANN v3

- **Opt-in format** — `ann_version` on L0, `centroids-routing.bin`, optional `centroids-l2-*`; env `OPENPUFFER_ANN_VERSION=3` / CLI `--ann-version 3`.
- **Incremental maintenance** — split/merge/reassign + scheduled rebuild (`maintenance_passes`); dual-read v2 segments during rollout.
- **Re-rank** — `OPENPUFFER_ANN_RERANK` / `--ann-rerank`: exact vectors from probed clusters over ANN shortlist.
- **Quality gates (lib / `#[ignore]`)** — v3 recall ≥ v2 + 0.05 @ 10k; index object count < 500 @ 100k; recall@10 ≥ 0.90 @ 100k; re-rank recall@10 ≥ 0.92 @ 10k.

### Recall

- **`POST /v1/namespaces/{name}/recall`** — ANN vs exhaustive `recall@k` (`num`, `top_k`, `vector_field`); `measure_recall` / `RecallMetrics` for benches and ops.
- **Filter-aware ANN pool** — attribute filters applied before ANN candidate selection in recall path.
- **Integration** — MinIO HTTP shape test (`avg_recall ≥ 0.85`); filtered recall HTTP test.

### Bench

- **`bench` feature** — `tests/bench_cold.rs`, `scripts/bench-1m.sh`, `docs/BENCHMARKS.md`.
- **Artifacts** — `benchmarks/results/baseline-10k.json`, `cold-50k-v3.json`; probe env table in BENCHMARKS.
- **CI tiers** — 10k bench job in CI; nightly 100k; 1M script → `benchmarks/results/1m-aws.json` (manual AWS).
- **Mid-tier** — `stress_50k` / `fifty_thousand_docs_v3_cold_probed_validation`: v3 cold, recall@10 ≥ 0.86, roundtrips ≤ 4, 175 index objects @ 50k MinIO.
- **Facts** — program gates under `# index/ann`, `# query/cold`, `# bench`; `facts check` 17/17 for ann/cold/bench tags.

### Ops

- **Prometheus** (`metrics` feature) — cold S3 keys fetched, ANN probed clusters, cold-query latency; mirrored in query `performance` JSON where applicable.
- **Compaction + cold restart** — integration gates for index cursor / cold query after compaction.
- **Docs** — ARCHITECTURE (cold planner, v3 layout, risks), COMPARISON (measured 10k/50k), README quickstart (v3, recall, cache flags).
- **Anneal** — shared WAL replay / probed decode helpers (`wal_commit_replay_from`, `decode_l0_by_field_from_fetched`); −133 LOC in cold/recall/indexer paths.

### Limitations

- **1M cold @ AWS** not validated in CI — run `scripts/bench-1m.sh` and commit `1m-aws.json` when available.
- v3 + incremental maintenance are **SPFresh-inspired**, not TurboPuffer production ANN; **v2 remains the default** for new indexes unless v3 is enabled at serve/index time.

See [docs/PLAN_SPFRESH_AND_COLD_1M.md](docs/PLAN_SPFRESH_AND_COLD_1M.md), [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

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
- **Vector ANN (v2 default)**: simplified two-level k-means (L0/L1 centroids + clusters), k-means++ init, configurable coarse/fine probes; `cosine_distance` / `euclidean_squared`; `[N]f32` and `[N]f16` schema.
- **Filters**: attribute filter index segments; intersect before scoring.
- Incremental segment merges; fair multi-namespace indexer round-robin.
- `index_cursor` in meta tracks merge progress; queries scan **unindexed WAL tail** under strong consistency.

### Query

- Hybrid `rank_by` (`Sum` / `Product`), BM25, ANN, attribute filters (`Eq`, `In`, `And`, …).
- `consistency: "strong"` (default) vs `"eventual"` (skips WAL tail on pinned views for lower latency).
- `order_by` tie-break after ranking; `performance` block (candidates, ratio, **`storage_roundtrips`**, billing estimates).
- Optional `--cache-dir` disk mirror + `POST /v1/namespaces/{name}/warm` view pin.

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
- Throughput and merge semantics are v1 simplifications; not validated at turbopuffer scale.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and [docs/COMPARISON.md](docs/COMPARISON.md) for design detail and gap list.

## [0.1.0] — earlier

Initial release: turbopuffer-shaped HTTP API with per-document JSON objects on S3 (`docs/{id}.json`, manifest). Superseded by 0.2.0 storage layout.