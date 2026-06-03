# openpuffer vs turbopuffer — honest comparison

This document compares **[openpuffer](https://github.com/av/openpuffer)** (self-hosted, open-source) with **[turbopuffer](https://turbopuffer.com)** (managed vector + FTS database). It is written for operators and contributors deciding whether openpuffer’s turbopuffer-aligned architecture is sufficient—not as a sales sheet.

For implementation detail see [ARCHITECTURE.md](ARCHITECTURE.md). For API shapes see [turbopuffer’s docs](https://turbopuffer.com/docs).

**Last refreshed:** SPFresh + cold @ 1M program (commit `9b65712`); treat this as **architecture v0.2** maturity (crate **0.2.0**), not a v1.0 product claim.

---

## Architecture maturity (v0.2)

openpuffer started as a turbopuffer-shaped API and is now a **real WAL + async index-on-S3 system**. The table below is how we describe maturity internally—not semver on the crate.

| Dimension | v0.1 (early) | **v0.2 (current)** | turbopuffer (reference) |
|-----------|--------------|-------------------|-------------------------|
| **System of record** | WAL + meta; legacy JSON fallback | **WAL + `meta.json` + `index/` only**; integration tests forbid `docs/*.json` | Production WAL + index layout |
| **Write path** | Basic append | Group commit, CAS, 1/s cap, compaction snapshot, CRC WAL wire format | Production batching + fleet scale |
| **Indexer** | Inline / stub | Background fair scheduler, FTS + filter + 2-level ANN, incremental segments | Dedicated indexer pool + SPFresh maintenance |
| **Query path** | Replay-all-WAL risk | Planner + indexed candidates + strong tail; **query-driven cold load** (`plan_cold_query`, ≤4 roundtrips); warm + eventual fast path | Tuned cold/warm at 1M+ docs |
| **Verification** | HTTP-only tests | **MinIO testcontainers + S3 byte asserts** (WAL decode, centroids, compaction deletes) | Internal + customer SLOs |
| **Operability** | README | Deep health, export, warm, limits, optional Prometheus, docker dev/test compose, **multi-replica integration-tested** | Control plane, auth, regions, billing |
| **Production readiness** | Prototype | **Private-network / staging / fork base** | Managed SLA product |

**What v0.2 means in practice:** you can run **multiple stateless `serve` processes** against one bucket (integration-tested: instance B queries A’s indexed writes; cross-instance warm/export), inspect `openpuffer/{ns}/wal|index|meta.json`, and get turbopuffer-like strong reads without a separate database. You should still **benchmark your bucket, region, and probes** before betting a business on ANN recall or cold latency.

---

## Shared model (what we deliberately copied)

Both systems treat **object storage as the system of record** for each namespace:

| Concept | turbopuffer | openpuffer |
|---------|-------------|------------|
| Durable writes | Append-only **WAL** under `{namespace}/wal/` | `openpuffer/{ns}/wal/{seq:08}.bin` (v1: `[0x01][bincode][crc32]`) |
| Commit point | `meta.json` CAS (`wal_commit_seq`) | Same: S3 conditional PUT on `meta.json` |
| Index lag | Async merge into `index/`; queries see unindexed WAL | `index_cursor` + strong tail scan `(index_cursor, wal_commit_seq]` |
| Write ACK | After durable WAL commit (not after index build) | Group-commit buffer → one WAL PUT + meta CAS → HTTP 200 |
| Query planner | Indexed candidates + filter intersect + score | FTS BM25, two-level ANN probe, hybrid `Sum`/`Product`, filters |
| Consistency default | Strong | `consistency: "strong"` (default) |
| Fast warm path | NVMe + pinned views; eventual sub-10ms | `--cache-dir` + `POST …/warm` + `consistency: "eventual"` on pinned view |

openpuffer is **not** a thin HTTP shim over JSON files: integration tests assert real S3 keys (`meta.json`, `wal/*`, `index/*`), decode WAL bytes from MinIO, and verify compaction removes old segments.

---

## What openpuffer implements today

### Storage & write path

- **WAL-only durability** — no `docs/{id}.json` / `manifest.json` legacy layout.
- **WAL integrity** — v1 segments: version byte + bincode + CRC32; legacy segments still replay; `OPENPUFFER_WAL_CORRUPT_POLICY` (`fail` | `skip`).
- **Group commit** — time- and batch-sized flush (`OPENPUFFER_WRITE_MAX_DELAY_MS`, `OPENPUFFER_WRITE_MAX_BATCH_OPS`).
- **Strong consistency** — ACK only after WAL PUT + successful `meta.json` CAS.
- **Per-namespace write serialization** — in-process commit lock + S3 `If-Match` CAS (safe multi-client, one WAL seq at a time).
- **~1 WAL commit / second / namespace** — enforced `min_commit_interval` (same default cap as turbopuffer’s 1/s story, but not the same throughput at fleet scale; see gaps).
- **WAL compaction** — when fully indexed: `wal/snapshot.bin`, prune old segments, cold load = snapshot + tail (integration-tested on MinIO).
- **Writes**: `upsert_rows` / `upsert_columns`, `deletes`, `patch_rows` / `patch_columns`, `delete_by_filter`, `patch_by_filter`, `schema` (`uuid`, `[]uuid`, `datetime`, vectors), `distance_metric`, **`upsert_condition`**, **`patch_condition`**, **`delete_condition`** (filter DSL + `$ref_new`; MinIO integration-tested), `return_affected_ids`, `copy_from_namespace`, `branch_from_namespace`, optional **`block_until_indexed`** (wait for indexer, 30s cap).
- **Limits** — namespace name validation, 64 MiB body, 10k rows/request, 5k filter-batch cap (with `delete_by_filter_allow_partial` / `patch_by_filter_allow_partial` + `rows_remaining`).
- **Write billing estimate** — `billing.billable_logical_bytes_written` on write responses.

### Background indexer

- **Decoupled from ACK** — `BackgroundIndexer` wakes after flush; fair round-robin across namespaces with lag priority (~2s slices).
- **FTS** — BM25 inverted segments (`fts-{seg}.bin`), incremental `apply_delta`, segment chains in meta; tokenizer: Unicode NFKC, letter/number runs, English stopwords, optional Porter stem (`OPENPUFFER_FTS_STEM`).
- **Filters** — inverted `(field, value) → doc_id` segments; intersect before ANN/FTS scoring.
- **Vector ANN (SPFresh-inspired)** — v2 default: two-level k-means (`centroids-l0`, `centroids-l1-*`, `clusters-*`). Optional **v3** (`OPENPUFFER_ANN_VERSION=3`): routing table + L2 splits, incremental split/merge/reassign, scheduled rebuild; dual-read v2/v3; lib gates recall@10 ≥ 0.90 @ 100k, v3 ≥ v2 + 0.05 @ 10k, object count < 500 @ 100k.
- **ANN re-rank** — `OPENPUFFER_ANN_RERANK` / `--ann-rerank` (exact vectors from view; recall@10 ≥ 0.92 @ 10k lib gate).
- **Recall API** — `POST /v1/namespaces/{name}/recall` (`avg_recall`, `avg_ann_count`, `avg_exhaustive_count`; MinIO integration ≥ 0.85 on 10k).
- **f16 vectors** — schema `[N]f16`, packed cluster storage, f32 scoring at query time.
- **Multi-vector columns** — more than one indexed vector field per namespace (per-field centroid keys under `index/{field}/`).

### Query path

- **`rank_by`**: vector ANN, BM25, hybrid `Sum` / `Product` with min-max normalization.
- **Filters**: `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `In`, `And`, `Or`.
- **`order_by`** tie-break after scoring.
- **`include_vectors`** + `vector_encoding` (`float` | `base64`); base64 f32/f16 on upsert.
- **`consistency`**: `strong` (WAL tail + `catch_up`) vs `eventual` (indexed snapshot only on pinned views; no WAL I/O on warm path).
- **Performance block** — `candidates`, `candidates_ratio`, `exhaustive_search_count`, `scored`, `storage_roundtrips`, `query_execution_us`; billing estimates in `performance.billing`.
- **Cold S3 batching** — [`plan_cold_query`](ARCHITECTURE.md#cold-query-s3_batch-roundtrips): bootstrap L0+FTS+filter, then **probed** L1/clusters only (`fetch_cold_vector_probed`); `storage_roundtrips ≤ 4` on caught-up 10k (bench + integration); not a full-index cold fetch.

### Operations API

- `GET /health`, `GET /health?deep=1` (S3 probe).
- `GET /v1/namespaces`, `GET /v1/namespaces/{name}` (`index_status`, `unindexed_bytes`, `approx_row_count`).
- `POST /v1/namespaces/{name}/warm`, export at `wal_commit_seq`, `DELETE` namespace.
- **Consistent errors** — `{"error":"…","status":"error"}` (`ApiErrorResponse`) on validation and planner failures.
- **Optional Prometheus** — `GET /metrics` with `--features metrics` (`wal_commits_total`, `index_lag_segments`, `s3_get_total`, query duration histogram).
- **Stateless horizontal scale** — multiple `serve` processes, one bucket, no shared RAM; WAL CAS + S3 as coordination. Integration test `multi_instance_stateless_integration`: concurrent A+B, cross-read/query/export, S3 WAL/meta/centroid asserts. **Warm pins remain per process** (not fleet-wide stickiness).

### Testing & dev ergonomics

- **`.facts` program gates** — `facts check --tags "ann or cold"` (10 spec facts, all `@implemented`); broader sheet via `facts check`.
- **MinIO integration** — 51 scenarios in `integration_s3` (`cargo test -F integration`), including **Phase A 10k gates**: `cold_vector_query_cluster_gets_bounded_by_probe_plan`, `s3_cold_query_reports_roundtrips_on_minio`, `ten_thousand_docs_indexed_query`, `recall_http_response_shape_on_minio`.
- **`bench` feature** — `bench_cold_10k_baseline`, `bench_cold_10k_storage_roundtrips_at_most_four` (CI); `bench_cold_100k_nightly` (`#[ignore]`).
- Plus `#[ignore]` external-S3 smoke and optional **`stress_50k`** (`cargo test -F large_stress --test stress_50k -- --ignored`).
- **S3 harness** — `tests/common/s3_harness.rs`: Head/List/Get, decode WAL/meta, assert centroids per vector field, compaction object deletion, multi-instance key parity.
- `docker-compose.yml` (dev MinIO) + `docker-compose.test.yml` + `scripts/run-integration-s3.sh` (local + external endpoint).
- Perf regression: 5k-doc ANN `candidates_ratio < 12%`; 10k-doc namespace indexing integration (~tens of seconds on MinIO); **optional 50k-doc stress** (5×10k batches, `candidates_ratio < 20%`, ~40–45s total on a typical dev machine with `--release`, not an SLA).

---

## What turbopuffer has that openpuffer still lacks

### Platform & operations

| Gap | turbopuffer | openpuffer |
|-----|-------------|------------|
| **Hosting** | Managed regions, LB, query/indexer fleet | Single binary you run; no control plane |
| **Multi-tenancy** | Many orgs per `./tpuf`; enterprise isolation | One deployment = your bucket; no org routing |
| **Regional routing** | [Regions](https://turbopuffer.com/docs/regions), latency-aware placement | You pick one S3 endpoint |
| **Query stickiness** | First query pins namespace to a node for NVMe locality | **Any `serve` instance** can serve any namespace (multi-instance tested); warm pin is **per process** only — no fleet pinning product |
| **Dedicated indexers** | Separate `./tpuf indexer` pool + indexing queue on S3 | Indexer is a background task inside `serve` |
| **Security** | API keys, permissions, audit logs, private networking, CMEK | No auth layer; no encryption beyond what S3 provides |
| **Billing product** | Metered billing, dashboards | JSON estimates only (`performance.billing`, write billing) |
| **Backups / DR** | Documented backup story | You own bucket versioning/replication |
| **Status / SLOs** | Published p50 cold/warm (e.g. 1M docs cold ~400–874ms, warm ~14ms) | No production SLOs; MinIO integration timings ≠ AWS at scale |
| **CI for external S3** | Managed pipelines | Scripts exist; no hosted CI job in-repo |

### Index & search engine depth

| Gap | turbopuffer | openpuffer |
|-----|-------------|------------|
| **True SPFresh** | Production centroid hierarchy, segment merges, object-storage–tuned layout | **v3** routing + L2 splits + incremental maintenance; still not turbopuffer’s full SPFresh fleet |
| **Graph ANN** | Centroid-based (not HNSW) by design | Same family; v3 improves recall vs v2 (+0.05 @ 10k gate) but fewer tuning years than prod |
| **Cold latency at 1M docs** | ~400–500ms class (marketing/docs) | **Not validated at 1M on AWS** — manual path: [`scripts/bench-1m.sh`](../scripts/bench-1m.sh), record `benchmarks/results/1m-aws.json`. **10k MinIO:** `storage_roundtrips` 2, p50 ~919ms ([`baseline-10k.json`](../benchmarks/results/baseline-10k.json)); probed load (not full 351-key index GET). **100k nightly:** `bench_cold_100k_nightly` (`#[ignore]`, recall@10 ≥ 0.88). |
| **FTS sophistication** | Production BM25 + optimizations | BM25 + improved tokenizer; simpler merge chain than turbopuffer at scale |
| **Filter expressiveness** | Broader DSL (e.g. text ops, more combinators) | v1: scalar compares + `In` + `And`/`Or` only; no `Contains`/`Glob`/etc. |
| **Recall API** | [`POST …/recall`](https://turbopuffer.com/docs/recall) | **`POST /v1/namespaces/{name}/recall`** implemented; not every turbopuffer recall option |
| **Patch vector in place** | turbopuffer patch vector support | **Vector fields cannot be patched** (400 on vector patch attrs) |
| **Conditional writes** | Full conditional write surface + edge cases | **`upsert_condition`**, **`patch_condition`**, **`delete_condition`** with filter DSL + `$ref_new`; not every turbopuffer write edge case |
| **Namespace pinning product** | [Pinning](https://turbopuffer.com/docs/pinning) for residency/latency | Warm LRU in-process only |
| **Chunking guides** | First-class chunking docs | Bring your own chunking |

### Throughput & limits

| Area | turbopuffer (typical) | openpuffer v0.2 |
|------|----------------------|-----------------|
| Write payload | Up to **512 MiB** / request | **64 MiB** hard cap |
| Effective ingest | High batching inside 1/s WAL commits; docs cite **~10k+ vectors/s** class at scale | **~1 WAL commit/s/ns** hard cap; practical throughput is batch-size × commits/s, not cloud-scale |
| Eventual staleness | Documented up to ~**1 hour** worst case in product docs | Index-lag bounded by your indexer speed; no hour-scale contract |
| Auth / RBAC | Yes | None (assume private network or edge auth) |

---

## Performance expectations (honest)

**Do not expect openpuffer to match turbopuffer’s published latencies** without your own tuning and hardware:

- **Cold query** on a large namespace still means multiple S3 roundtrips (~100ms+ each on real WAN). Caught-up 10k on MinIO: **2** logical `storage_roundtrips`, probed cluster GETs (integration: bounded by probe plan, not `num_fine_total`). **1M on AWS** is manual only — see [`docs/BENCHMARKS.md`](BENCHMARKS.md).
- **Warm query** after `POST …/warm` + disk cache can avoid `GetObject` on index segments; `eventual` on a pinned view skips WAL I/O—similar *shape* to turbopuffer’s sub-10ms goal, but measured only in integration tests on MinIO, not at million-doc scale.
- **ANN recall** — lib gates: v2 `recall@10 > 0.75` (1k×32); v3 ≥ v2 + 0.05 @ 10k; re-rank ≥ 0.92 @ 10k; 100k ≥ 0.90 (`#[ignore]`); HTTP recall ≥ 0.85 @ 10k MinIO. Production recall still depends on probes, `ann_version`, and rebuild cadence.
- **Indexing lag** under load: fair scheduler + 2s slices mean multi-namespace workloads share indexer time; a 10k-doc namespace can take ~30s+ to catch up on MinIO (see integration test notes). Optional **50k-doc stress** (`large_stress`) indexes in ~40–45s on a dev machine with `--release`; not validated on AWS at that scale.

---

## When to use openpuffer

- You want a **turbopuffer-shaped API** and **WAL + index-on-S3 architecture** you can run yourself (MinIO, AWS, R2, etc.).
- You need **horizontally scaled query nodes** (multiple `serve` replicas, one bucket) without a separate database—only object storage + optional per-node cache.
- You need **strong read-your-writes** without a separate database—only object storage + optional local cache.
- You are building **RAG / hybrid search** prototypes where **~1 commit/s/namespace** and simplified ANN are acceptable.
- You want **inspectable S3 layout** (`wal/`, `index/`, `meta.json`) for debugging, compliance-friendly storage, or custom tooling.
- You plan to **fork or extend** the Rust codebase (indexer fairness, probes, compaction, metrics are all in-tree).
- Your threat model allows **private-network deployment without API auth** (or you will add auth at the edge).

---

## When **not** to use openpuffer

- You need **managed SLAs**, global regions, and **million-document cold queries in hundreds of milliseconds** out of the box.
- You require **production auth**, CMEK, audit logs, SOC2-style controls, or multi-tenant billing.
- Write throughput must sustain **thousands of commits per second per namespace** or very large single-request payloads (>64 MiB).
- You need **full turbopuffer API coverage** (advanced filters, vector patch, recall HTTP API, pinning product, every write edge case beyond the conditional-write trio).
- **ANN recall and latency are business-critical** without you operating benchmarks, probe tuning, and capacity planning on your bucket/region.
- You want **HNSW/graph indexes** or SPFresh-equivalent behavior without operating your own index research loop.

For those cases, use **[turbopuffer](https://turbopuffer.com)** (managed) or treat openpuffer as an **architecture reference implementation (v0.2)**, not a drop-in replacement.

---

## Quick reference matrix

| | turbopuffer | openpuffer (v0.2) |
|---|-------------|-------------------|
| **License / deployment** | Commercial SaaS (+ enterprise BYOC) | Open source; self-hosted binary |
| **System of record** | S3 (per region) | Your S3-compatible bucket |
| **API compatibility** | Canonical | Core write/query/metadata/export/warm subset |
| **Architecture fidelity** | Production SPFresh + FTS + filters | WAL + 2-level k-means ANN + FTS + filters (simplified, verified on MinIO) |
| **Maturity** | Production product | Reference/staging architecture; **facts-driven** ann/cold gates + S3-proof integration (incl. cold probe bound, recall HTTP, multi-instance, 50k optional stress) |
| **Best fit** | Production apps at scale | Dev, staging, private cloud, learning, forks |

---

## References

- [turbopuffer Architecture](https://turbopuffer.com/docs/architecture)
- [turbopuffer Tradeoffs](https://turbopuffer.com/docs/tradeoffs)
- [turbopuffer Limits](https://turbopuffer.com/docs/limits)
- [openpuffer ARCHITECTURE.md](ARCHITECTURE.md)