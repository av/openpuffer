# openpuffer architecture (turbopuffer-aligned)

openpuffer is a stateless HTTP API backed by S3-compatible object storage. The on-disk layout and write path follow [turbopuffer’s architecture](https://turbopuffer.com/docs/architecture): a per-namespace **write-ahead log (WAL)** on object storage, asynchronous **index** builds under `index/`, and **namespace metadata** that tracks the index cursor and WAL commit point.

## Object storage layout

Each namespace is rooted at `openpuffer/{namespace}/`:

```
openpuffer/{ns}/
├── meta.json              # NamespaceMeta (index cursor, WAL commit, schema, distance metric)
├── wal/
│   ├── 00000001.bin       # WalEntry (bincode): batched upserts + patches + deletes
│   ├── 00000002.bin
│   └── ...
└── index/
    ├── fts-{segment_id:08}.bin   # BM25 inverted postings (bincode)
    ├── centroids-l0.bin            # coarse ANN centroid table (bincode)
    ├── centroids-l1-{coarse_id:08}.bin  # fine centroids per coarse cell
    ├── clusters-{fine_id:08}.bin   # doc id + vector per fine cluster
    └── ...
```

All durable state uses **WAL + index segments only**. There is no per-document `docs/{id}.json` or `manifest.json` layout. Namespaces without `meta.json` are treated as empty.

## Namespace metadata (`meta.json`)

| Field | Role |
|-------|------|
| `index_cursor` | Last WAL sequence number fully merged into `index/` (0 until indexer runs) |
| `wal_commit_seq` | Last durably committed WAL file (`wal/{seq:08}.bin`) |
| `schema` | JSON schema hints (attributes, vector dims) |
| `distance_metric` | ANN distance: `cosine_distance` (default) or `euclidean_squared` |
| `fts_segment_id` / `fts_segment_ids` | Latest FTS segment + generation chain (one file per indexer pass) |
| `filter_segment_id` / `filter_segment_ids` | Latest filter segment + chain |
| `vector_segment_id` / `vector_segment_ids` | WAL seq when `centroids-l0.bin` + `centroids-l1-*` + `clusters-*.bin` were last written |
| `vector_field` | Indexed vector attribute (e.g. `embedding`) |
| `dimensions` | Vector dimensionality (0 if no ANN index) |

Updates use **conditional PUT** (`If-Match` / `If-None-Match`) so concurrent writers serialize commits (compare-and-swap on `meta.json`). A per-namespace commit mutex ([`commit_lock.rs`](../src/commit_lock.rs)) ensures only one WAL append + meta CAS runs at a time; indexer CAS re-reads `meta.json` before commit so `wal_commit_seq` is never regressed.

## Write path

1. API accepts turbopuffer-shaped JSON (`upsert_rows`, `upsert_columns`, `deletes`).
2. Enqueue in per-namespace **write buffer** (`buffer.rs`): group commit by time (default 1s) or batch size.
3. Flush builds one `WalEntry` batch (upserts + attribute patches + deletes). Patches merge into existing docs on replay; missing ids ignored; vector fields cannot be patched.
4. Assign `seq = wal_commit_seq + 1`.
5. **PUT** `wal/{seq:08}.bin` (bincode payload) — durable before ACK.
6. **CAS** update `meta.json`: set `wal_commit_seq = seq` (retries on `PreconditionFailed`).
7. **Wake** the async background indexer (non-blocking).
8. HTTP ACK only after steps 5–6 succeed (**strong consistency**). Index build is **not** on the ACK path.

**Write response** (turbopuffer [`write` response](https://turbopuffer.com/docs/write) subset): `rows_affected`, optional `rows_upserted` / `rows_patched` / `rows_deleted`, and `billing.billable_logical_bytes_written` (v1 estimate: 64 bytes × affected rows per request).

### Write throughput limits (v1)

Aligned with [turbopuffer limits](https://turbopuffer.com/docs/limits) (per-namespace write throughput is capped in production; openpuffer enforces a **hard** WAL commit rate).

| Limit | Default | Notes |
|-------|---------|-------|
| Group-commit delay | 1s (`OPENPUFFER_WRITE_MAX_DELAY_MS`) | Batches in-memory writes per namespace before one WAL PUT |
| **Max WAL commits / s / ns** | **1** (`min_commit_interval` = `max_delay`) | **Enforced:** after a durable commit, the next flush waits until 1s elapsed; writes during the wait batch into memory. `flush_all` on shutdown bypasses this. |
| Max batch ops | 512 (`OPENPUFFER_WRITE_MAX_BATCH_OPS`) | Schedules a flush when upserts + patches + deletes reach this count, but the flush still waits on the 1s commit cooldown if a commit happened recently |
| Meta CAS retries | 8 | Exponential backoff 50ms × attempt; orphan WAL segment deleted on conflict |
| Concurrent HTTP writers | Serialized per namespace | Commit lock + S3 `If-Match` on `meta.json`; safe parallel clients, one WAL seq at a time |
| Practical throughput | ~1 WAL commit / s / ns | Raising `OPENPUFFER_WRITE_MAX_DELAY_MS` lowers commit rate; lowering it below 1000ms also lowers `min_commit_interval` (same env var drives both today) |
| Payload size | No explicit cap in openpuffer | turbopuffer allows up to 512 MiB per write request |

## Local disk cache (index segments)

Optional NVMe-style cache for **index objects only** ([`cache.rs`](../src/cache.rs)) — not WAL or `meta.json`.

| Setting | Behavior |
|---------|----------|
| `--cache-dir` / `OPENPUFFER_CACHE_DIR` (default `/tmp/openpuffer-cache`) | Mirror `index/*` under `{cache_dir}/{bucket}/{s3_key}` |
| Empty `--cache-dir=""` | Memory-only: every index load uses S3 directly |

**Warm vs cold query:**

| Path | What happens |
|------|----------------|
| **Cold** | No disk cache (`--cache-dir=""`): batched parallel S3 plan ([`s3_batch.rs`](../src/s3_batch.rs)) — round 1 `centroids-l0`+FTS, round 2 filter+L1+clusters (query path fetches only probed L1/clusters), WAL tail in extra rounds; `performance.storage_roundtrips` on query JSON |
| **Cold (cached)** | No local file (or etag stale after HEAD): `GetObject` from S3, write bytes + etag sidecar |
| **Warm** | Local file + HEAD etag match: serve from disk (no `GetObject`) |
| **Prefetch** | After `centroids-l0.bin` loads, background task fetches all `centroids-l1-*` + `clusters-*.bin` into cache for follow-up ANN queries |

**Indexer:** each `PutObject` for FTS / filter / vector segments writes S3 first, then populates the cache from the response etag.

## Warm cache (`POST /v1/namespaces/{name}/warm`)

Turbopuffer [`hint_cache_warm`](https://turbopuffer.com/docs/warm-cache) analogue ([`warm.rs`](../src/warm.rs)):

1. **Prefetch** `meta.json`, current FTS/filter/`centroids-l0` + all L1/cluster segments, and recent WAL tail (up to 128 segments) into the disk cache via HEAD+GET when needed.
2. **Pin** a fully caught-up [`NamespaceView`](../src/view.rs) in the in-process LRU map ([`view_cache.rs`](../src/view_cache.rs), default max 32 namespaces via `OPENPUFFER_MAX_PINNED_NAMESPACES`).
3. Return `200` JSON with `duration_ms`, segment counts, and `s3_get_count` for the warm pass.

After warm, queries against the same process reuse the pinned view (no WAL replay) and index loads hit disk cache (HEAD only, no `GetObject` when etags match).

## Read path / query planner

**Implemented** ([`search.rs`](../src/search.rs)):

1. Load `meta.json` + FTS/vector `index/` segments (disk cache when enabled) + in-process [`NamespaceView`](../src/view.rs) (incremental WAL catch-up).
2. **Candidate generation** per `rank_by` subtree:
   - BM25: FTS posting-list union + top posting hits (indexed docs only).
   - Vector: ANN centroid probe + cluster member ids (indexed docs only).
   - **Sum:** union child candidate sets; **Product:** intersection.
3. **Score only candidates** (no full-namespace scan when indexes exist).
4. **Hybrid** `Sum` / `Product`: min-max normalize each sub-ranker over the shared candidate set, then combine.
5. Merge sort + `top_k` truncation.

**Consistency** (query body `consistency`):

| Mode | Behavior |
|------|----------|
| `strong` (default) | Indexed segments + exhaustive scoring for doc ids touched in unindexed WAL tail `(index_cursor, wal_commit_seq]`. Queries never block on the background indexer. |
| `eventual` | Indexed segments only; skip WAL tail scan (faster; very recent writes may be invisible until indexed). |

**Performance observability** (turbopuffer [`performance`](https://turbopuffer.com/docs/query#responsefield-performance) subset):

| Field | Meaning |
|-------|---------|
| `approx_namespace_size` | Live doc count in the namespace view |
| `candidates` | Doc ids after candidate generation (+ filters) |
| `candidates_ratio` | `candidates / approx_namespace_size` — indexed ANN/FTS should stay ≪ 1 |
| `exhaustive_search_count` | Docs scanned via full-namespace fallback or unindexed WAL tail |
| `query_execution_us` | Planner + score time on the server |

HTTP headers: `X-Openpuffer-Candidates`, `X-Openpuffer-Candidates-Fraction` (`candidates/total`).

Regression guard: `cargo test --features perf` runs `tests/perf_namespace.rs` (5k docs, 128-dim ANN) and asserts `candidates_ratio < 0.12` (not O(n); two-level probe fetches ~`4×2` fine clusters plus tail).

## Background indexer

Indexing is **decoupled from the write hot path** ([`BackgroundIndexer`](../src/indexer.rs)):

1. After each durable WAL flush, the write buffer **notifies** the indexer (`wake`) — no await on index build.
2. A single tokio background task runs continuously:
   - Waits up to **500ms** or until notified.
   - Processes namespaces in the pending queue plus any namespace where `index_cursor < wal_commit_seq` (S3 prefix scan).
3. For each lagging namespace:
   - Read WAL from `index_cursor + 1` through `wal_commit_seq`.
   - **FTS:** load latest `fts-{id}.bin`, `apply_delta` from WAL batch only, write `fts-{seq}.bin`, append `fts_segment_ids`.
   - **Filter:** load latest filter segment, `apply_delta` (no full WAL replay).
   - **Vector ANN:** load L0/L1 + clusters, `apply_delta` (two-level nearest-centroid assign for new docs); **full k-means rebuild** only when `doc_count > num_fine_total × 4` (see below). Writes `centroids-l0.bin`, `centroids-l1-{coarse}.bin`, `clusters-{fine}.bin`, appends `vector_segment_ids`.
   - CAS-advance `index_cursor` in `meta.json`.
   - When caught up and `wal_commit_seq > 10`, compact indexed WAL: write `wal/snapshot.bin`, delete `wal/{seq:08}.bin` for `seq <= index_cursor - 3`, set `wal_snapshot_seq` in meta. Cold load replays snapshot + tail WAL only.
4. On indexer errors: log, re-queue namespace, **retry** on next tick — writes are never blocked.

**Metadata API:** `GET /v1/namespaces/{name}` exposes `index_cursor`, `wal_commit_seq`, `approx_row_count` (view or WAL replay), `unindexed_bytes` (HEAD sum of WAL segments after `index_cursor`), and `index_status` (`catching_up` | `up_to_date`). `GET /v1/namespaces` returns the same per-ns fields except row count.

**Health:** `GET /health` returns `{status:"ok"}`. With `?deep=1`, probes S3 (`HeadBucket`, list `openpuffer/`, HEAD canary `meta.json` when a namespace exists); returns `{status:"degraded",s3:"unavailable"}` with HTTP 503 if S3 is unreachable.

### Vector ANN (SPFresh-inspired, two-level)

[turbopuffer SPFresh](https://turbopuffer.com/docs/architecture) uses hierarchical centroid clustering, re-ranking, and object-storage–friendly segments. openpuffer implements a **two-level** hierarchy:

| SPFresh / turbopuffer | openpuffer v1 |
|----------------------|---------------|
| Multi-level centroid hierarchy | **L0** coarse k-means (≤16 cells) + **L1** fine k-means per coarse (`k_fine ≈ √n_cell`, cap 256) |
| Incremental cluster maintenance | Incremental two-level assign; full rebuild when `doc_count > num_fine_total × 4` |
| Re-rank with fresh vectors from WAL | Cluster files store doc vectors; tail WAL scored exhaustively |
| Many small segments + merges | `centroids-l0.bin` + `centroids-l1-{coarse:08}.bin` + `clusters-{fine:08}.bin` |

**Build:** coarse k-means partitions the namespace; each coarse cell runs fine k-means (10 iterations, seed = first *k* doc vectors). Global fine ids are `fine_counts` offsets in L0. Cluster files list `(doc_id, vector)` for cosine (or negated L2²) scoring.

**Query (`rank_by: ["vector", "ANN", field, query]`):**

1. Load `centroids-l0.bin`, pick top-*C* coarse centroids (*C* = 4 by default; all coarse if `num_fine_total ≤ 32`).
2. Fetch probed `centroids-l1-{coarse}.bin` files, then top-*F* fine centroids per coarse (*F* = 2 by default).
3. Fetch only matching `clusters-*.bin` objects from S3 (cold query round 2 uses this subset; full cold load fetches all L1 + clusters).
4. Score members in probed clusters; merge with **exhaustive** cosine on docs touched in unindexed WAL tail `(index_cursor, wal_commit_seq]`.

Distance uses `distance_metric` from `meta.json` (`cosine_distance` default).

## Query phases (turbopuffer model)

| Phase | Source | Iteration |
|-------|--------|-----------|
| Metadata | `meta.json` | 1 |
| Indexed ANN / FTS | `index/*` | 3–4 (implemented) |
| Unindexed tail | `wal/*.bin` after `index_cursor` | 1 (full replay), 6 (tail only) |
| Filters | `index/filter-{seg:08}.bin` | 7 (implemented) |

**Filters (`filters: ["field", "Eq", value]` …):**

1. Parse turbopuffer-style DSL: `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `In`, `And`, `Or` (unsupported ops → 400).
2. Load inverted filter segment from S3: `(field, value_key) → doc_id` sets.
3. Intersect filter matches with ANN/FTS **candidates before scoring**; WAL tail docs re-evaluated with `eval_filter` under strong consistency.

## Consistency

- **Strong (default):** query after write reads up to `wal_commit_seq`.
- **Eventual:** optional later; may skip latest WAL for lower latency.

## References

- [turbopuffer Architecture](https://turbopuffer.com/docs/architecture)
- [Write API](https://turbopuffer.com/docs/write)
- [Query API](https://turbopuffer.com/docs/query)
- [Namespace metadata](https://turbopuffer.com/docs/metadata)