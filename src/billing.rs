//! Logical-byte estimates for turbopuffer-shaped billing fields (v1 observability).

use crate::models::{Document, QueryRow};
use serde_json::Value;
use std::collections::HashMap;

/// Fallback average doc size when the namespace is empty (matches write-path estimate).
pub const DEFAULT_AVG_DOC_LOGICAL_BYTES: u64 = 64;

/// Approximate logical bytes for a JSON value (UTF-8 serialized size).
pub fn value_logical_bytes(v: &Value) -> u64 {
    serde_json::to_string(v)
        .map(|s| s.len() as u64)
        .unwrap_or(0)
}

/// Approximate logical bytes for a stored document (id + attributes).
pub fn document_logical_bytes(doc: &Document) -> u64 {
    let id_bytes = doc.id.len() as u64;
    let attr_bytes: u64 = doc
        .attributes
        .values()
        .map(value_logical_bytes)
        .sum();
    id_bytes.saturating_add(attr_bytes)
}

/// Mean logical bytes per document in the namespace view.
pub fn avg_document_logical_bytes(docs: &HashMap<String, Document>) -> u64 {
    if docs.is_empty() {
        return DEFAULT_AVG_DOC_LOGICAL_BYTES;
    }
    let total: u64 = docs.values().map(document_logical_bytes).sum();
    (total / docs.len() as u64).max(1)
}

/// Logical bytes billed for candidate examination: `candidates × avg_doc_size`.
pub fn billable_logical_bytes_queried(candidate_count: u64, avg_doc_bytes: u64) -> u64 {
    candidate_count.saturating_mul(avg_doc_bytes)
}

/// Logical bytes in returned rows (id + projected attributes).
pub fn query_row_logical_bytes(row: &QueryRow) -> u64 {
    let mut n = row.id.len() as u64;
    if let Some(attrs) = row.attributes.as_ref() {
        n = n.saturating_add(
            attrs
                .iter()
                .map(|(k, v)| (k.len() as u64).saturating_add(value_logical_bytes(v)))
                .sum::<u64>(),
        );
    }
    n
}

/// Sum of [`query_row_logical_bytes`] across result rows.
pub fn billable_logical_bytes_returned(rows: &[QueryRow]) -> u64 {
    rows.iter().map(query_row_logical_bytes).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Document;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn avg_doc_bytes_uses_namespace_mean() {
        let mut docs = HashMap::new();
        docs.insert(
            "a".into(),
            Document {
                id: "a".into(),
                attributes: [("x".into(), json!("hello"))].into(),
            },
        );
        docs.insert(
            "b".into(),
            Document {
                id: "b".into(),
                attributes: [("x".into(), json!("worldwide"))].into(),
            },
        );
        let avg = avg_document_logical_bytes(&docs);
        assert!(avg > 1);
        assert!(avg < 200);
    }

    #[test]
    fn queried_bytes_scales_with_candidates() {
        assert_eq!(billable_logical_bytes_queried(10, 64), 640);
        assert_eq!(billable_logical_bytes_queried(0, 64), 0);
    }

    #[test]
    fn returned_bytes_sums_row_payload() {
        let row = QueryRow {
            id: "doc-1".into(),
            attributes: Some(HashMap::from([("title".into(), json!("hi"))])),
            dist: Some(1.0),
        };
        assert!(query_row_logical_bytes(&row) > 5);
        assert_eq!(
            billable_logical_bytes_returned(std::slice::from_ref(&row)),
            query_row_logical_bytes(&QueryRow {
                id: "doc-1".into(),
                attributes: Some(HashMap::from([("title".into(), json!("hi"))])),
                dist: None,
            })
        );
    }
}