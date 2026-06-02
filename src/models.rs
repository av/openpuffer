use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

pub const ROOT_PREFIX: &str = "openpuffer/";

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
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}

#[derive(Debug, Serialize)]
pub struct NamespaceSummary {
    pub id: String,
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