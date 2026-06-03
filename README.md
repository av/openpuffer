# openpuffer

Stateless vector and full-text search server with S3-compatible object storage. Turbopuffer-compatible HTTP API for core write/query paths.

## Features

- Approximate nearest-neighbor vector search (in-process cosine distance)
- BM25 full-text search
- Hybrid `rank_by` queries (`Sum` / `Product`)
- Single static binary — no sidecar services
- All durable state in S3 under `openpuffer/{namespace}/` (`meta.json`, `wal/`, `index/` — no per-doc JSON files)

## Build

```bash
cargo build --release
```

## Run

Create your bucket first, then:

```bash
openpuffer serve \
  --listen 0.0.0.0:8080 \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-bucket mybucket \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin
```

Environment variables: `OPENPUFFER_S3_ENDPOINT`, `OPENPUFFER_S3_BUCKET`, `OPENPUFFER_S3_REGION`, `OPENPUFFER_S3_ACCESS_KEY`, `OPENPUFFER_S3_SECRET_KEY`.

## API

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | readiness |
| GET | `/v1/namespaces` | list namespaces |
| POST | `/v2/namespaces/{name}` | upsert / delete |
| POST | `/v2/namespaces/{name}/query` | vector, FTS, hybrid |
| DELETE | `/v2/namespaces/{name}` | delete namespace |

## Test

```bash
cargo test
cargo test -F integration   # requires Docker (MinIO testcontainers)
```

## License

MIT OR Apache-2.0