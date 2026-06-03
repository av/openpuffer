//! Namespace export: consistent document snapshot at `wal_commit_seq` from WAL replay.
//!
//! turbopuffer recommends query-by-id paging for export; openpuffer exposes a dedicated
//! endpoint that reconstructs rows from the in-memory view (WAL replay + optional pin).

use crate::models::{Document, ExportRow};
use std::collections::HashMap;

/// Default page size (matches turbopuffer export example `limit=10_000`).
pub const DEFAULT_EXPORT_LIMIT: usize = 10_000;

/// Max rows per export request.
pub const MAX_EXPORT_LIMIT: usize = 10_000;

#[derive(Debug, Clone)]
pub struct ExportPage {
    pub wal_commit_seq: u64,
    pub rows: Vec<ExportRow>,
    /// Set when `rows.len() == limit` so the client can pass this as `last_id` for the next page.
    pub next_last_id: Option<String>,
}

/// Build one export page: ids sorted ascending, optional `id > last_id` cursor.
pub fn export_page(
    docs: &HashMap<String, Document>,
    wal_commit_seq: u64,
    last_id: Option<&str>,
    limit: usize,
) -> ExportPage {
    let limit = limit.clamp(1, MAX_EXPORT_LIMIT);
    let mut ids: Vec<&String> = docs.keys().collect();
    ids.sort_unstable();
    let rows: Vec<ExportRow> = ids
        .into_iter()
        .filter(|id| last_id.map(|l| id.as_str() > l).unwrap_or(true))
        .take(limit)
        .filter_map(|id| docs.get(id).map(document_to_export_row))
        .collect();
    let next_last_id = if rows.len() == limit {
        rows.last().map(|r| r.id.clone())
    } else {
        None
    };
    ExportPage {
        wal_commit_seq,
        rows,
        next_last_id,
    }
}

fn document_to_export_row(doc: &Document) -> ExportRow {
    ExportRow {
        id: doc.id.clone(),
        attributes: doc.attributes.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Document;
    use serde_json::json;

    fn doc(id: &str) -> Document {
        Document {
            id: id.into(),
            attributes: [("k".into(), json!(id))].into(),
        }
    }

    #[test]
    fn export_page_sorted_and_paginates() {
        let mut docs = HashMap::new();
        for id in ["c", "a", "b"] {
            docs.insert(id.into(), doc(id));
        }
        let p1 = export_page(&docs, 3, None, 2);
        assert_eq!(p1.wal_commit_seq, 3);
        assert_eq!(p1.rows.len(), 2);
        assert_eq!(p1.rows[0].id, "a");
        assert_eq!(p1.rows[1].id, "b");
        assert_eq!(p1.next_last_id.as_deref(), Some("b"));

        let p2 = export_page(&docs, 3, p1.next_last_id.as_deref(), 2);
        assert_eq!(p2.rows.len(), 1);
        assert_eq!(p2.rows[0].id, "c");
        assert!(p2.next_last_id.is_none());
    }

    #[test]
    fn export_page_respects_last_id_exclusive() {
        let mut docs = HashMap::new();
        docs.insert("a".into(), doc("a"));
        docs.insert("b".into(), doc("b"));
        let page = export_page(&docs, 1, Some("a"), 100);
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].id, "b");
    }

    #[test]
    fn export_empty_namespace() {
        let docs = HashMap::new();
        let page = export_page(&docs, 0, None, 100);
        assert!(page.rows.is_empty());
        assert!(page.next_last_id.is_none());
    }
}