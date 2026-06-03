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

/// Legacy namespace manifest (read-only fallback; new writes use WAL + meta.json).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Manifest {
    pub doc_ids: Vec<String>,
    #[serde(default)]
    pub schema_hints: Value,
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
    pub deletes: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpsertRow {
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
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
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
}

/// GET /v1/namespaces/{name} — simplified turbopuffer metadata shape.
#[derive(Debug, Serialize)]
pub struct NamespaceMetadata {
    pub id: String,
    pub index_cursor: u64,
    pub wal_commit_seq: u64,
    pub unindexed_bytes: u64,
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

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub rows: Vec<QueryRow>,
}

pub fn namespace_prefix(name: &str) -> String {
    format!("{ROOT_PREFIX}{name}/")
}

pub fn manifest_key(name: &str) -> String {
    format!("{ROOT_PREFIX}{name}/manifest.json")
}

pub fn doc_key(name: &str, id: &str) -> String {
    format!("{ROOT_PREFIX}{name}/docs/{id}.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_long_ids() {
        let id = "x".repeat(MAX_DOC_ID_BYTES + 1);
        assert!(validate_doc_id(&id).is_err());
    }
}