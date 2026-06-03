//! openpuffer — stateless vector + FTS server backed by S3-compatible storage.
//!
//! Durable writes use a turbopuffer-style WAL on object storage (`wal/{seq:08}.bin`
//! + `meta.json`); see `docs/ARCHITECTURE.md`.
//!
//! HTTP routes (turbopuffer-compatible):
//!   GET  /health
//!   GET  /v1/namespaces
//!   GET  /v1/namespaces/{name}      — namespace metadata (index cursor, WAL commit, unindexed bytes)
//!   POST /v1/namespaces/{name}/warm — prefetch index + WAL cache, pin in-memory view
//!   POST /v2/namespaces/{name}        — write (upsert_rows, schema, deletes, delete_by_filter)
//!   POST /v2/namespaces/{name}/query  — vector, FTS, hybrid query
//!   DELETE /v2/namespaces/{name}
//!
//! CLI: `openpuffer serve` with flags:
//!   --s3-endpoint --s3-bucket --s3-region --s3-access-key --s3-secret-key
//! Env: OPENPUFFER_S3_ENDPOINT, OPENPUFFER_S3_BUCKET, OPENPUFFER_S3_REGION,
//!      OPENPUFFER_S3_ACCESS_KEY, OPENPUFFER_S3_SECRET_KEY,
//!      OPENPUFFER_CACHE_DIR (index segment disk cache; empty = disabled)

pub mod api;
pub mod buffer;
pub mod cache;
pub mod config;
pub mod filter;
pub mod index;
pub mod indexer;
pub mod meta;
pub mod models;
pub mod namespace;
pub mod schema;
pub mod search;
pub mod storage;
pub mod view;
pub mod view_cache;
pub mod warm;
pub mod wal;

pub use api::{router, AppState};
pub use config::AppConfig;