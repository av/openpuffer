# openpuffer vs turbopuffer — honest comparison

This document compares **[openpuffer](https://github.com/av/openpuffer)** (self-hosted, open-source) with **[turbopuffer](https://turbopuffer.com)** (managed vector + FTS database). It is written for operators and contributors deciding whether openpuffer’s turbopuffer-aligned architecture is sufficient—not as a sales sheet.

For implementation detail see [ARCHITECTURE.md](ARCHITECTURE.md). For API shapes see [turbopuffer’s docs](https://turbopuffer.com/docs).

---

## Shared model (what we deliberately copied)

Both systems treat **object storage as the system of record** for each namespace:

| Concept | turbopuffer | openpuffer |
|---------|-------------|------------|
| Durable writes | Append-only **WAL** under `{namespace}/wal/` | `openpuffer/{ns}/wal/{seq:08}.bin` (bincode `WalEntry`) |
| Commit point | `meta.json` CAS (`wal_commit_seq`) | Same: S3 conditional PUT on `meta.json` |
| Index lag | Async merge into `index/`; queries see unindexed WAL | `index_cursor` + strong tail scan `(index_cursor, wal_commit_seq]` |
| Write ACK | After durable WAL commit (not after index build) | Group-commit buffer → one WAL PUT + meta CAS → HTTP 200 |
| Query planner | Indexed candidates + filter intersect + score | FTS BM25, two-level ANN probe, hybrid `Sum`/`Product`, filters |
| Consistency default | Strong | `consistency: "strong"` (default) |
| Fast warm path | NVMe + pinned views; eventual sub-10ms | `--cache-dir` + `POST …/warm` + `consistency: "eventual"` on pinned view |

openpuffer is **not** a thin HTTP shim over JSON files anymore: integration tests assert real S3 keys (`meta.json`, `wal/*`, `index/*`) and decode WAL bytes from MinIO.

---

## What openpuffer implements today

### Storage & write path

- **WAL-only durability** — no `docs/{id}.json` / `manifest.json` legacy layout.
- **Group commit** — time- and batch-sized flush (`OPENPUFFER_WRITE_MAX_DELAY_MS`, `OPENPUFFER_WRITE_MAX_BATCH_OPS`).
- **Strong consistency** — ACK only after WAL PUT + successful `meta.json` CAS.
- **Per-namespace write serialization** — in-process commit lock + S3 `If-Match` CAS (safe multi-client, one WAL seq at a time).
- **~1 WAL commit / second / namespace** — enforced `min_commit_interval` (same default as turbopuffer’s 1/s cap, but not the same throughput story; see gaps).
- **WAL compaction** — when fully indexed: `wal/snapshot.bin`, prune old segments, cold load = snapshot + tail.
- **Writes**: `upsert_rows` / `upsert_columns`, `deletes`, `patch_rows` / `patch_columns`, `delete_by_filter`, `patch_by_filter`, `schema`, `distance_metric`, `upsert_condition`, `return_affected_ids`, `copy_from_namespace`, `branch_from_namespace`.
- **Limits** — namespace name validation, 64 MiB body, 10k rows/request, 5k filter-batch cap (with `*_allow_partial`).

### Background indexer

- **Decoupled from ACK** — `BackgroundIndexer` wakes after flush; fair round-robin across namespaces with lag priority.
- **FTS** — BM25 inverted segments (`fts-{seg}.bin`), incremental `apply_delta`, segment chains in meta.
- **Filters** — inverted `(field, value) → doc_id` segments; intersect before ANN/FTS scoring.
- **Vector ANN (SPFresh-inspired, simplified)** — two-level k-means: `centroids-l0.bin`, `centroids-l1-{coarse}.bin`, `clusters-{fine}.bin`; k-means++ init; incremental assign + full rebuild when `doc_count > 4 × fine_centroids`; probe tuning (`OPENPUFFER_ANN_COARSE_PROBE` / `FINE`, defaults 4/2).
- **f16 vectors** — schema `[N]f16`, packed cluster storage, f32 scoring at query time.
- **Multi-vector columns** — more than one indexed vector field per namespace (per-field centroid keys).

### Query path

- **`rank_by`**: vector ANN, BM25, hybrid `Sum` / `Product` with min-max normalization.
- **Filters**: `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `In`, `And`, `Or`.
- **`order_by`** tie-break after scoring.
- **`include_vectors`** + `vector_encoding` (`float` | `base64`); base64 f32/f16 on upsert.
- **`consistency`**: `strong` (WAL tail + `catch_up`) vs `eventual` (indexed snapshot only on pinned views).
- **Performance block** — `candidates`, `candidates_ratio`, `exhaustive_search_count`, `storage_roundtrips`, `query_execution_us`; billing estimates in `performance.billing`.
- **Cold S3 batching** — two-round parallel fetch (`s3_batch.rs`) when `--cache-dir=""`.

### Operations API

- `GET /health`, `GET /health?deep=1` (S3 probe).
- `GET /v1/namespaces`, `GET /v1/namespaces/{name}` (`index_status`, `unindexed_bytes`, `approx_row_count`).
- `POST /v1/namespaces/{name}/warm`, export at `wal_commit_seq`, `DELETE` namespace.
- **Stateless `serve`** — multiple processes, one bucket; optional local index cache only.

### Testing & dev ergonomics

- **67+ `.facts` checks**, 127+ unit tests, 33+ MinIO integration tests (Head/Get/decode WAL, multi-instance bucket sharing, compaction S3 proofs).
- `docker-compose` MinIO + `scripts/dev-up.sh` / `dev-serve.sh`.
- Perf regression: 5k-doc ANN `candidates_ratio < 12%`.

---

## What turbopuffer has that openpuffer still lacks

### Platform & operations

| Gap | turbopuffer | openpuffer |
|-----|-------------|------------|
| **Hosting** | Managed regions, LB, query/indexer fleet | Single binary you run; no control plane |
| **Multi-tenancy** | Many orgs per `./tpuf`; enterprise isolation | One deployment = your bucket; no org routing |
| **Regional routing** | [Regions](https://turbopuffer.com/docs/regions), latency-aware placement | You pick one S3 endpoint |
| **Query stickiness** | First query pins namespace to a node for NVMe locality | Any `serve` instance; warm pin is **per process** only |
| **Dedicated indexers** | Separate `./tpuf indexer` pool + indexing queue on S3 | Indexer is a background task inside `serve` |
| **Security** | API keys, permissions, audit logs, private networking, CMEK | No auth layer; no encryption beyond what S3 provides |
| **Billing product** | Metered billing, dashboards | Estimates in JSON only (`performance.billing`) |
| **Backups / DR** | Documented backup story | You own bucket versioning/replication |
| **Status / SLOs** | Published p50 cold/warm (e.g. 1M docs cold ~400–874ms, warm ~14ms) | No production SLOs; MinIO integration timings ≠ AWS at scale |

### Index & search engine depth

| Gap | turbopuffer | openpuffer |
|-----|-------------|------------|
| **True SPFresh** | Production centroid hierarchy, segment merges, object-storage–tuned layout | **Two-level k-means simplification**; rebuild threshold, not full SPFresh maintenance |
| **Graph ANN** | Centroid-based (not HNSW) by design | Same family, but fewer levels, coarser segments, lower tuned recall budget |
| **Cold latency at 1M docs** | ~400–500ms class (marketing/docs) | Not validated at 1M; 10k-doc integration ~tens of seconds to index; cold = many S3 roundtrips |
| **FTS sophistication** | Production BM25 + optimizations | BM25 over bincode postings; simpler merge chain |
| **Filter expressiveness** | Broader DSL (e.g. text ops, more combinators) | v1: scalar compares + `In` + `And`/`Or` only; no `Contains`/`Glob`/etc. |
| **Recall API** | [`POST …/recall`](https://turbopuffer.com/docs/recall) | Unit-test recall@10 only; no HTTP recall endpoint |
| **Patch vector in place** | turbopuffer patch vector support | **Vector fields cannot be patched** (400 on vector patch attrs) |
| **Conditional writes** | Full conditional write surface | `upsert_condition` only; no `patch_condition` / `delete_condition` |
| **Namespace pinning product** | [Pinning](https://turbopuffer.com/docs/pinning) for residency/latency | Warm LRU in-process only |
| **Chunking guides** | First-class chunking docs | Bring your own chunking |

### Throughput & limits

| Area | turbopuffer (typical) | openpuffer v1 |
|------|----------------------|---------------|
| Write payload | Up to **512 MiB** / request | **64 MiB** hard cap |
| Effective ingest | High batching inside 1/s WAL commits; docs cite **~10k+ vectors/s** class at scale | **~1 WAL commit/s/ns** hard cap; practical throughput is batch-size × commits/s, not cloud-scale |
| Eventual staleness | Documented up to ~**1 hour** worst case in product docs | Index-lag bounded by your indexer speed; no hour-scale contract |
| Auth / RBAC | Yes | None (assume private network) |

---

## Performance expectations (honest)

**Do not expect openpuffer to match turbopuffer’s published latencies** without your own tuning and hardware:

- **Cold query** on a large namespace still means multiple S3 roundtrips (~100ms+ each on real WAN). We batch into ~2 index rounds + WAL tail, but have not benchmarked 1M documents on AWS.
- **Warm query** after `POST …/warm` + disk cache can avoid `GetObject` on index segments; `eventual` on a pinned view skips WAL I/O—similar *shape* to turbopuffer’s sub-10ms goal, but measured only in integration tests on MinIO, not at million-doc scale.
- **ANN recall** is regression-tested (`recall@10 > 0.75` on 1k×32 synthetic); production recall depends on probes, data distribution, and rebuild cadence.
- **Indexing lag** under load: fair scheduler + 2s slices mean multi-namespace workloads share indexer time; a 10k-doc namespace can take ~30s+ to catch up on MinIO (see integration test notes).

---

## When to use openpuffer

- You want a **turbopuffer-shaped API** and **WAL + index-on-S3 architecture** you can run yourself (MinIO, AWS, R2, etc.).
- You need **strong read-your-writes** without a separate database—only object storage + optional local cache.
- You are building **RAG / hybrid search** prototypes where **~1 commit/s/namespace** and simplified ANN are acceptable.
- You want **inspectable S3 layout** (`wal/`, `index/`, `meta.json`) for debugging, compliance-friendly storage, or custom tooling.
- You plan to **fork or extend** the Rust codebase (indexer fairness, probes, compaction are all in-tree).
- Your threat model allows **private-network deployment without API auth** (or you will add auth at the edge).

---

## When **not** to use openpuffer

- You need **managed SLAs**, global regions, and **million-document cold queries in hundreds of milliseconds** out of the box.
- You require **production auth**, CMEK, audit logs, SOC2-style controls, or multi-tenant billing.
- Write throughput must sustain **thousands of commits per second per namespace** or very large single-request payloads (>64 MiB).
- You need **full turbopuffer API coverage** (advanced filters, `patch_condition`, vector patch, recall HTTP API, pinning product, every write edge case).
- **ANN recall and latency are business-critical** without you operating benchmarks, probe tuning, and capacity planning on your bucket/region.
- You want **HNSW/graph indexes** or SPFresh-equivalent behavior without operating your own index research loop.

For those cases, use **[turbopuffer](https://turbopuffer.com)** (managed) or treat openpuffer as an **architecture reference implementation**, not a drop-in replacement.

---

## Quick reference matrix

| | turbopuffer | openpuffer |
|---|-------------|------------|
| **License / deployment** | Commercial SaaS (+ enterprise BYOC) | Open source; self-hosted binary |
| **System of record** | S3 (per region) | Your S3-compatible bucket |
| **API compatibility** | Canonical | Core write/query/metadata/export/warm subset |
| **Architecture fidelity** | Production SPFresh + FTS + filters | WAL + 2-level k-means ANN + FTS + filters (simplified) |
| **Best fit** | Production apps at scale | Dev, staging, private cloud, learning, forks |

---

## References

- [turbopuffer Architecture](https://turbopuffer.com/docs/architecture)
- [turbopuffer Tradeoffs](https://turbopuffer.com/docs/tradeoffs)
- [turbopuffer Limits](https://turbopuffer.com/docs/limits)
- [openpuffer ARCHITECTURE.md](ARCHITECTURE.md)