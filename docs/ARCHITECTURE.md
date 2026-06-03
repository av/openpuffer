# openpuffer architecture (turbopuffer-aligned)

openpuffer is a stateless HTTP API backed by S3-compatible object storage. The on-disk layout and write path follow [turbopuffer’s architecture](https://turbopuffer.com/docs/architecture): a per-namespace **write-ahead log (WAL)** on object storage, asynchronous **index** builds under `index/`, and **namespace metadata** that tracks the index cursor and WAL commit point.

## Object storage layout

Each namespace is rooted at `openpuffer/{namespace}/`:

```
openpuffer/{ns}/
├── meta.json              # NamespaceMeta (index cursor, WAL commit, schema, distance metric)
├── wal/
│   ├── 00000001.bin       # WalEntry (bincode): batched upserts + deletes
│   ├── 00000002.bin
│   └── ...
└── index/
    ├── fts-{segment_id:08}.bin   # BM25 inverted postings (bincode)
    ├── centroids.bin               # ANN centroid table (bincode)
    ├── clusters-{centroid_id:08}.bin  # doc id + vector per cluster
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
| `vector_segment_id` | WAL seq when `centroids.bin` + `clusters-*.bin` were last written |
| `vector_field` | Indexed vector attribute (e.g. `embedding`) |
| `dimensions` | Vector dimensionality (0 if no ANN index) |

Updates use **conditional PUT** (`If-Match` / `If-None-Match`) so concurrent writers serialize commits (compare-and-swap on `meta.json`).

## Write path

1. API accepts turbopuffer-shaped JSON (`upsert_rows`, `upsert_columns`, `deletes`).
2. Enqueue in per-namespace **write buffer** (`buffer.rs`): group commit by time (default 1s) or batch size.
3. Flush builds one `WalEntry` batch (upserts + deletes).
4. Assign `seq = wal_commit_seq + 1`.
5. **PUT** `wal/{seq:08}.bin` (bincode payload) — durable before ACK.
6. **CAS** update `meta.json`: set `wal_commit_seq = seq` (retries on `PreconditionFailed`).
7. **Wake** the async background indexer (non-blocking).
8. HTTP ACK only after steps 5–6 succeed (**strong consistency**). Index build is **not** on the ACK path.

## Read path / query planner

**Implemented** ([`search.rs`](../src/search.rs)):

1. Load `meta.json` + FTS/vector `index/` segments + in-process [`NamespaceView`](../src/view.rs) (incremental WAL catch-up).
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

## Background indexer

Indexing is **decoupled from the write hot path** ([`BackgroundIndexer`](../src/indexer.rs)):

1. After each durable WAL flush, the write buffer **notifies** the indexer (`wake`) — no await on index build.
2. A single tokio background task runs continuously:
   - Waits up to **500ms** or until notified.
   - Processes namespaces in the pending queue plus any namespace where `index_cursor < wal_commit_seq` (S3 prefix scan).
3. For each lagging namespace:
   - Read WAL from `index_cursor + 1` through `wal_commit_seq`.
   - **FTS:** merge upserts/deletes into `fts-{seq}.bin`, set `fts_segment_id`.
   - **Vector ANN:** rebuild centroid/cluster layout from all docs at `index_cursor` (see below), write `centroids.bin` + `clusters-{id}.bin`, set `vector_segment_id`, `vector_field`, `dimensions`.
   - CAS-advance `index_cursor` in `meta.json`.
4. On indexer errors: log, re-queue namespace, **retry** on next tick — writes are never blocked.

**Metadata API:** `GET /v1/namespaces/{name}` and `GET /v1/namespaces` (per-ns fields) expose `index_cursor`, `wal_commit_seq`, and approximate `unindexed_bytes` (sum of WAL object sizes in the unindexed tail).

### Vector ANN (SPFresh-inspired, simplified)

[turbopuffer SPFresh](https://turbopuffer.com/docs/architecture) uses hierarchical centroid clustering, re-ranking, and object-storage–friendly segments. openpuffer v1 implements a **minimal subset**:

| SPFresh / turbopuffer | openpuffer v1 |
|----------------------|---------------|
| Multi-level centroid hierarchy | Single-level k-means (`k ≈ √n`, cap 256) |
| Incremental cluster maintenance | Full rebuild from WAL `1..=index_cursor` on each index pass |
| Re-rank with fresh vectors from WAL | Cluster files store doc vectors; tail WAL scored exhaustively |
| Many small segments + merges | One `centroids.bin` + `clusters-{centroid_id:08}.bin` per namespace |

**Build:** k-means (10 iterations, seed = first *k* doc vectors) assigns each document vector to a centroid; each cluster file lists `(doc_id, vector)` for cosine (or negated L2²) scoring.

**Query (`rank_by: ["vector", "ANN", field, query]`):**

1. Load `centroids.bin`, pick top-*M* centroids nearest to the query (*M* = 8 by default; all centroids if *k* ≤ 32).
2. Fetch only those `clusters-*.bin` objects from S3 (not a full namespace scan).
3. Score members in probed clusters; merge with **exhaustive** cosine on docs touched in unindexed WAL tail `(index_cursor, wal_commit_seq]`.

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