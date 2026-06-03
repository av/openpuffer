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
└── index/                 # (iter 3+) ANN centroids/clusters, FTS postings, filter indexes
    ├── centroids.bin
    ├── clusters-*.bin
    └── ...
```

Legacy layout (`manifest.json`, `docs/{id}.json`) is **read-only fallback** for namespaces created before WAL; new writes go **only** to the WAL.

## Namespace metadata (`meta.json`)

| Field | Role |
|-------|------|
| `index_cursor` | Last WAL sequence number fully merged into `index/` (0 until indexer runs) |
| `wal_commit_seq` | Last durably committed WAL file (`wal/{seq:08}.bin`) |
| `schema` | JSON schema hints (attributes, vector dims) |
| `distance_metric` | ANN distance: `cosine_distance` (default) or `euclidean_squared` |

Updates use **conditional PUT** (`If-Match` / `If-None-Match`) so concurrent writers serialize commits (compare-and-swap on `meta.json`).

## Write path

1. API accepts turbopuffer-shaped JSON (`upsert_rows`, `upsert_columns`, `deletes`).
2. Enqueue in per-namespace **write buffer** (`buffer.rs`): group commit by time (default 1s) or batch size.
3. Flush builds one `WalEntry` batch (upserts + deletes).
4. Assign `seq = wal_commit_seq + 1`.
5. **PUT** `wal/{seq:08}.bin` (bincode payload) — durable before ACK.
6. **CAS** update `meta.json`: set `wal_commit_seq = seq` (retries on `PreconditionFailed`).
7. HTTP ACK only after steps 5–6 succeed (**strong consistency**).

## Read path (current vs target)

**Iteration 2 (implemented):** in-process [`NamespaceView`](../src/view.rs) caches `docs` + `last_applied_wal_seq`. Queries call `catch_up()` to fetch only `wal/{seq}.bin` for `seq > last_applied_wal_seq` instead of replaying `1..=N` every time. Legacy namespaces still load `manifest.json` + `docs/`.

**Target (iter 3+):**

1. Load `meta.json` + relevant `index/` segments.
2. Search indexed data (ANN + BM25 + filters).
3. **Exhaustive scan** of unindexed WAL tail: `seq in (index_cursor, wal_commit_seq]`.
4. Optional NVMe/memory cache on query nodes ([warm cache](https://turbopuffer.com/docs/warm-cache)).

Strong consistency: after a successful write, the next query sees data replayed from committed WAL.

## Background indexer (planned)

Separate loop (not in iter 1):

- Read WAL from `index_cursor + 1` through `wal_commit_seq`.
- Merge into `index/` (SPFresh-style centroids/clusters for vectors, inverted BM25 for FTS).
- Advance `index_cursor` in `meta.json` via CAS.

## Query phases (turbopuffer model)

| Phase | Source | Iteration |
|-------|--------|-----------|
| Metadata | `meta.json` | 1 |
| Indexed ANN / FTS | `index/*` | 3–4 |
| Unindexed tail | `wal/*.bin` after `index_cursor` | 1 (full replay), 6 (tail only) |
| Filters | attribute indexes | 7 |

## Consistency

- **Strong (default):** query after write reads up to `wal_commit_seq`.
- **Eventual:** optional later; may skip latest WAL for lower latency.

## References

- [turbopuffer Architecture](https://turbopuffer.com/docs/architecture)
- [Write API](https://turbopuffer.com/docs/write)
- [Query API](https://turbopuffer.com/docs/query)
- [Namespace metadata](https://turbopuffer.com/docs/metadata)