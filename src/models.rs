use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

pub const ROOT_PREFIX: &str = "openpuffer/";

/// Max length for a document id used as a single S3 path segment (MinIO/S3 limit is 255).
pub const MAX_DOC_ID_BYTES: usize = 255;

/// Validate document id before using it in an object key.
pub fn validate_doc_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("document id must not be empty".into());
    }
    if id.len() > MAX_DOC_ID_BYTES {
        return Err(format!(
            "document id exceeds maximum length of {} bytes",
            MAX_DOC_ID_BYTES
        ));
    }
    if id.contains('/') || id.contains('\\') || id.contains('\0') {
        return Err("document id must not contain path separators or null bytes".into());
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: String,
    #[serde(default)]
    pub attributes: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct WriteRequest {
    #[serde(default)]
    pub upsert_rows: Vec<UpsertRow>,
    #[serde(default)]
    pub upsert_columns: Option<Value>,
    #[serde(default)]
    pub patch_rows: Vec<PatchRow>,
    #[serde(default)]
    pub patch_columns: Option<Value>,
    #[serde(default)]
    pub deletes: Vec<String>,
    /// turbopuffer schema hints (`full_text_search`, `filterable`, `[N]f32`, …).
    #[serde(default)]
    pub schema: Option<Value>,
    /// Delete all documents matching this filter (same syntax as query `filters`).
    #[serde(default)]
    pub delete_by_filter: Option<Value>,
    /// When true, delete up to the server filter batch limit and set `rows_remaining`.
    #[serde(default)]
    pub delete_by_filter_allow_partial: bool,
    /// Patch all documents matching `filters` with `patch` attributes (`{ filters, patch }`).
    #[serde(default)]
    pub patch_by_filter: Option<Value>,
    /// When true, patch up to the server filter batch limit and set `rows_remaining`.
    #[serde(default)]
    pub patch_by_filter_allow_partial: bool,
    /// Copy all S3 objects from another namespace (destination must be empty).
    #[serde(default)]
    pub copy_from_namespace: Option<String>,
    /// Branch (COW clone) from another namespace — same S3 copy as `copy_from_namespace`.
    #[serde(default)]
    pub branch_from_namespace: Option<String>,
    /// Conditional upserts (turbopuffer): same filter DSL as query; missing docs always insert.
    #[serde(default)]
    pub upsert_condition: Option<Value>,
    /// Conditional patches (turbopuffer): same filter DSL; missing ids are ignored without evaluation.
    #[serde(default)]
    pub patch_condition: Option<Value>,
    /// Conditional deletes (turbopuffer): same filter DSL; missing ids are ignored; `$ref_new` is null.
    #[serde(default)]
    pub delete_condition: Option<Value>,
    /// ANN distance for the namespace: `cosine_distance` (default) or `euclidean_squared`.
    /// Stored in `meta.json` on first write; later writes must match.
    #[serde(default)]
    pub distance_metric: Option<String>,
    /// Include `upserted_ids` and `deleted_ids` in the write response.
    #[serde(default)]
    pub return_affected_ids: bool,
    /// When true, block the write response until the background indexer catches up
    /// (`index_cursor == wal_commit_seq`). Times out after 30s.
    #[serde(default)]
    pub block_until_indexed: bool,
}

#[derive(Debug, Deserialize)]
pub struct UpsertRow {
    pub id: String,
    #[serde(default)]
    pub attributes: HashMap<String, Value>,
}

/// Per-request write stats (turbopuffer write response subset).
#[derive(Debug, Clone, Default, Serialize)]
pub struct WriteStats {
    pub rows_upserted: u64,
    pub rows_patched: u64,
    pub rows_deleted: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upserted_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_ids: Option<Vec<String>>,
    /// True when a filter-based write hit the batch cap and more rows may match.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_remaining: Option<bool>,
}

impl WriteStats {
    pub fn rows_affected(&self) -> u64 {
        self.rows_upserted + self.rows_patched + self.rows_deleted
    }

    /// Approximate logical bytes written for billing observability.
    pub fn billable_logical_bytes_written(&self) -> u64 {
        self.rows_affected().saturating_mul(64)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WriteBilling {
    pub billable_logical_bytes_written: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WriteResponse {
    pub status: &'static str,
    pub namespace: String,
    pub rows_affected: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_upserted: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_patched: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_deleted: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upserted_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_remaining: Option<bool>,
    pub billing: WriteBilling,
}

impl WriteResponse {
    pub fn from_stats(namespace: String, stats: WriteStats) -> Self {
        let rows_affected = stats.rows_affected();
        let billing_bytes = stats.billable_logical_bytes_written();
        Self {
            status: "ok",
            namespace,
            rows_affected,
            rows_upserted: (stats.rows_upserted > 0).then_some(stats.rows_upserted),
            rows_patched: (stats.rows_patched > 0).then_some(stats.rows_patched),
            rows_deleted: (stats.rows_deleted > 0).then_some(stats.rows_deleted),
            upserted_ids: stats.upserted_ids,
            deleted_ids: stats.deleted_ids,
            rows_remaining: stats.rows_remaining,
            billing: WriteBilling {
                billable_logical_bytes_written: billing_bytes,
            },
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct PatchRow {
    pub id: String,
    #[serde(default)]
    pub attributes: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub rank_by: Value,
    #[serde(default)]
    pub top_k: Option<u32>,
    #[serde(default)]
    pub filters: Option<Value>,
    #[serde(default)]
    pub include_attributes: Option<Value>,
    /// `strong` (default): indexed segments + unindexed WAL tail. `eventual`: indexed only.
    #[serde(default)]
    pub consistency: Option<String>,
    /// Secondary sort after `rank_by` scoring, e.g. `["priority", "desc"]` (turbopuffer attribute order shape).
    #[serde(default)]
    pub order_by: Option<Value>,
    /// `true` or field name list — return vector columns in row attributes (turbopuffer query option).
    #[serde(default)]
    pub include_vectors: Option<Value>,
    /// `float` (default) or `base64` (little-endian f32) for vectors in the response.
    #[serde(default)]
    pub vector_encoding: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    /// Present when `?deep=1`: `"ok"` if S3 probes succeeded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deep: Option<bool>,
}

/// Background indexer state for a namespace (turbopuffer `index_status` subset).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexStatus {
    CatchingUp,
    UpToDate,
}

impl IndexStatus {
    pub fn from_meta(index_cursor: u64, wal_commit_seq: u64) -> Self {
        if index_cursor < wal_commit_seq {
            Self::CatchingUp
        } else {
            Self::UpToDate
        }
    }
}

#[derive(Debug, Serialize)]
pub struct NamespaceSummary {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_cursor: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_commit_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unindexed_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_status: Option<IndexStatus>,
}

/// GET /v1/namespaces/{name} — simplified turbopuffer metadata shape.
#[derive(Debug, Serialize)]
pub struct NamespaceMetadata {
    pub id: String,
    pub index_cursor: u64,
    pub wal_commit_seq: u64,
    /// Live document count from in-memory view or full WAL replay.
    pub approx_row_count: u64,
    pub unindexed_bytes: u64,
    pub index_status: IndexStatus,
}

#[derive(Debug, Serialize)]
pub struct NamespacesResponse {
    pub namespaces: Vec<NamespaceSummary>,
}

#[derive(Debug, Serialize)]
pub struct QueryRow {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, Value>>,
    #[serde(rename = "$dist", skip_serializing_if = "Option::is_none")]
    pub dist: Option<f64>,
}

/// Query billing estimates (turbopuffer `billing` subset; nested under `performance` in openpuffer v1).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct QueryBilling {
    /// Estimated logical bytes processed (`candidates × avg_doc_logical_bytes`).
    pub billable_logical_bytes_queried: u64,
    /// Sum of logical bytes in returned row payloads (id + projected attributes).
    pub billable_logical_bytes_returned: u64,
}

/// Query observability (turbopuffer `performance` object subset).
#[derive(Debug, Clone, Serialize)]
pub struct QueryPerformance {
    /// Documents in the namespace view at query time.
    pub approx_namespace_size: u64,
    /// Doc ids considered for scoring after candidate generation (and filters).
    pub candidates: u64,
    /// `candidates / approx_namespace_size` (0 when namespace empty).
    pub candidates_ratio: f64,
    /// Doc ids scored by the ranker.
    pub scored: u64,
    /// Docs examined via full-namespace scan or unindexed WAL tail (not index postings).
    pub exhaustive_search_count: u64,
    /// Server-side query planner time in microseconds.
    pub query_execution_us: u64,
    /// Logical S3 fetch rounds (parallel batch = 1 roundtrip; turbopuffer cold-query model).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_roundtrips: Option<u32>,
    /// v1 billing estimates (turbopuffer top-level `billing`; nested here for API stability).
    pub billing: QueryBilling,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub rows: Vec<QueryRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub performance: Option<QueryPerformance>,
}

/// One exported document row (`GET/POST /v1/namespaces/{name}/export`).
#[derive(Debug, Clone, Serialize)]
pub struct ExportRow {
    pub id: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, Value>,
}

/// JSON export response (default `format=json`).
#[derive(Debug, Serialize)]
pub struct ExportResponse {
    pub wal_commit_seq: u64,
    pub rows: Vec<ExportRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_last_id: Option<String>,
}

/// Optional body for `POST /v1/namespaces/{name}/recall`.
#[derive(Debug, Default, Deserialize)]
pub struct RecallRequest {
    /// Number of random query vectors to sample (default 25).
    #[serde(default = "default_recall_num")]
    pub num: usize,
    /// Neighbors per query for recall@k (default 10).
    #[serde(default = "default_recall_top_k")]
    pub top_k: usize,
    #[serde(default)]
    pub filters: Option<Value>,
}

fn default_recall_num() -> usize {
    25
}

fn default_recall_top_k() -> usize {
    10
}

/// turbopuffer recall response (`avg_recall`, `avg_ann_count`, `avg_exhaustive_count`).
#[derive(Debug, Clone, Copy, Serialize)]
pub struct RecallResponse {
    pub avg_recall: f64,
    pub avg_ann_count: f64,
    pub avg_exhaustive_count: f64,
}

impl From<crate::recall::RecallMetrics> for RecallResponse {
    fn from(m: crate::recall::RecallMetrics) -> Self {
        Self {
            avg_recall: m.avg_recall,
            avg_ann_count: m.avg_ann_count,
            avg_exhaustive_count: m.avg_exhaustive_count,
        }
    }
}

/// Optional body for `POST /v1/namespaces/{name}/export`.
#[derive(Debug, Default, Deserialize)]
pub struct ExportRequest {
    #[serde(default)]
    pub last_id: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    /// `json` (default) or `ndjson` (rows only, one JSON object per line).
    #[serde(default)]
    pub format: Option<String>,
}

/// turbopuffer-style API error body: `{"error": "...", "status": "error"}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApiErrorResponse {
    pub error: String,
    pub status: String,
}

impl ApiErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            status: "error".to_string(),
        }
    }
}

pub fn namespace_prefix(name: &str) -> String {
    format!("{ROOT_PREFIX}{name}/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_long_ids() {
        let id = "x".repeat(MAX_DOC_ID_BYTES + 1);
        assert!(validate_doc_id(&id).is_err());
    }

    #[test]
    fn write_stats_rows_affected_sums_ops() {
        let stats = WriteStats {
            rows_upserted: 3,
            rows_patched: 1,
            rows_deleted: 2,
            upserted_ids: None,
            deleted_ids: None,
            rows_remaining: None,
        };
        assert_eq!(stats.rows_affected(), 6);
        assert_eq!(stats.billable_logical_bytes_written(), 384);
    }

    #[test]
    fn api_error_response_serializes_turbopuffer_shape() {
        let body = ApiErrorResponse::new("namespace name invalid");
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["status"], "error");
        assert_eq!(json["error"], "namespace name invalid");
    }

    #[test]
    fn write_response_omits_zero_row_fields() {
        let resp = WriteResponse::from_stats(
            "ns".into(),
            WriteStats {
                rows_upserted: 2,
                upserted_ids: None,
                deleted_ids: None,
                ..Default::default()
            },
        );
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["rows_affected"], 2);
        assert_eq!(json["rows_upserted"], 2);
        assert!(json.get("rows_patched").is_none());
        assert!(json.get("rows_deleted").is_none());
        assert!(json.get("upserted_ids").is_none());
        assert!(json["billing"]["billable_logical_bytes_written"].as_u64().unwrap() > 0);
    }

    #[test]
    fn write_response_includes_affected_ids_when_present() {
        let resp = WriteResponse::from_stats(
            "ns".into(),
            WriteStats {
                rows_upserted: 1,
                rows_deleted: 1,
                upserted_ids: Some(vec!["a".into()]),
                deleted_ids: Some(vec!["b".into()]),
                ..Default::default()
            },
        );
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["upserted_ids"], serde_json::json!(["a"]));
        assert_eq!(json["deleted_ids"], serde_json::json!(["b"]));
    }
}