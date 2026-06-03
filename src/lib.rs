//! openpuffer — stateless vector + FTS server backed by S3-compatible storage.
//!
//! Durable writes use a turbopuffer-style WAL on object storage (`wal/{seq:08}.bin`
//! + `meta.json`); see `docs/ARCHITECTURE.md`.
//!
//! HTTP routes (turbopuffer-compatible):
//!   GET  /health
//!   GET  /v1/namespaces
//!   POST /v2/namespaces/{name}        — write (upsert_rows, upsert_columns, deletes)
//!   POST /v2/namespaces/{name}/query  — vector, FTS, hybrid query
//!   DELETE /v2/namespaces/{name}
//!
//! CLI: `openpuffer serve` with flags:
//!   --s3-endpoint --s3-bucket --s3-region --s3-access-key --s3-secret-key
//! Env: OPENPUFFER_S3_ENDPOINT, OPENPUFFER_S3_BUCKET, OPENPUFFER_S3_REGION,
//!      OPENPUFFER_S3_ACCESS_KEY, OPENPUFFER_S3_SECRET_KEY

pub mod api;
pub mod config;
pub mod meta;
pub mod models;
pub mod namespace;
pub mod search;
pub mod storage;
pub mod wal;

pub use api::{router, AppState};
pub use config::AppConfig;