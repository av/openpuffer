//! Namespace metadata: `openpuffer/{ns}/meta.json` with CAS commit point.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const META_RETRIES: u32 = 8;

/// ANN distance metric stored in namespace metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetric {
    #[default]
    CosineDistance,
    EuclideanSquared,
}

/// Durable namespace state on object storage (turbopuffer-style commit + index cursor).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamespaceMeta {
    /// Last WAL sequence fully merged into `index/` (0 until indexer runs).
    pub index_cursor: u64,
    /// Latest FTS segment id on S3 (`index/fts-{id:08}.bin`).
    #[serde(default)]
    pub fts_segment_id: u64,
    /// FTS segment generation chain (each indexer pass appends WAL seq of new segment file).
    #[serde(default)]
    pub fts_segment_ids: Vec<u64>,
    /// Latest vector index segment id (centroids + clusters written at this WAL seq).
    #[serde(default)]
    pub vector_segment_id: u64,
    /// Vector index generation chain (WAL seq when centroids/clusters were written).
    #[serde(default)]
    pub vector_segment_ids: Vec<u64>,
    /// Latest attribute filter index segment id (`index/filter-{id:08}.bin`).
    #[serde(default)]
    pub filter_segment_id: u64,
    /// Filter segment generation chain.
    #[serde(default)]
    pub filter_segment_ids: Vec<u64>,
    /// Indexed vector attribute name (e.g. `embedding`).
    #[serde(default)]
    pub vector_field: String,
    /// Vector dimensions for the ANN index (0 if no vector index).
    #[serde(default)]
    pub dimensions: u32,
    /// Last committed WAL file sequence (`wal/{seq:08}.bin`).
    pub wal_commit_seq: u64,
    /// WAL seq materialized in `wal/snapshot.bin` after compaction (0 = none).
    #[serde(default)]
    pub wal_snapshot_seq: u64,
    #[serde(default)]
    pub schema: Value,
    #[serde(default)]
    pub distance_metric: DistanceMetric,
}

impl Default for NamespaceMeta {
    fn default() -> Self {
        Self {
            index_cursor: 0,
            fts_segment_id: 0,
            fts_segment_ids: Vec::new(),
            vector_segment_id: 0,
            vector_segment_ids: Vec::new(),
            filter_segment_id: 0,
            filter_segment_ids: Vec::new(),
            vector_field: String::new(),
            dimensions: 0,
            wal_commit_seq: 0,
            wal_snapshot_seq: 0,
            schema: Value::Object(serde_json::Map::new()),
            distance_metric: DistanceMetric::default(),
        }
    }
}

pub fn meta_key(namespace: &str) -> String {
    format!("{}{namespace}/meta.json", crate::models::ROOT_PREFIX)
}

/// Next WAL sequence after a successful commit.
pub fn next_wal_seq(meta: &NamespaceMeta) -> u64 {
    meta.wal_commit_seq.saturating_add(1)
}

/// Append a segment id to a generation chain (dedupes consecutive duplicates).
pub fn push_segment_id(ids: &mut Vec<u64>, id: u64) {
    if id == 0 {
        return;
    }
    if ids.last() != Some(&id) {
        ids.push(id);
    }
}

/// Build updated metadata after appending WAL object `seq` (CAS payload).
pub fn meta_after_wal_commit(meta: &NamespaceMeta, seq: u64) -> Result<NamespaceMeta> {
    meta_after_wal_commit_with_schema(meta, seq, None)
}

/// Like [`meta_after_wal_commit`], optionally merging a write-time schema patch.
pub fn meta_after_wal_commit_with_schema(
    meta: &NamespaceMeta,
    seq: u64,
    schema_patch: Option<&Value>,
) -> Result<NamespaceMeta> {
    if seq != meta.wal_commit_seq.saturating_add(1) {
        return Err(anyhow!(
            "wal seq {seq} does not follow commit point {}",
            meta.wal_commit_seq
        ));
    }
    let mut next = meta.clone();
    next.wal_commit_seq = seq;
    if let Some(patch) = schema_patch {
        next.schema = crate::schema::merge_schema(&next.schema, patch);
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_json_roundtrip() {
        let meta = NamespaceMeta {
            index_cursor: 3,
            fts_segment_id: 3,
            fts_segment_ids: vec![3],
            filter_segment_id: 3,
            filter_segment_ids: vec![3],
            vector_segment_id: 3,
            vector_segment_ids: vec![3],
            vector_field: "embedding".into(),
            dimensions: 128,
            wal_commit_seq: 10,
            wal_snapshot_seq: 0,
            schema: serde_json::json!({"embedding": {"type": "[]f32"}}),
            distance_metric: DistanceMetric::EuclideanSquared,
        };
        let json = serde_json::to_vec(&meta).unwrap();
        let back: NamespaceMeta = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn meta_after_wal_commit_advances_seq() {
        let meta = NamespaceMeta::default();
        let next = meta_after_wal_commit(&meta, 1).unwrap();
        assert_eq!(next.wal_commit_seq, 1);
        assert_eq!(next.index_cursor, 0);
    }

    #[test]
    fn meta_after_wal_commit_merges_schema() {
        let meta = NamespaceMeta::default();
        let patch = serde_json::json!({
            "text": {"type": "string", "full_text_search": true},
            "embedding": "[128]f32"
        });
        let next = meta_after_wal_commit_with_schema(&meta, 1, Some(&patch)).unwrap();
        assert_eq!(next.schema["text"]["full_text_search"], serde_json::json!(true));
        assert_eq!(next.schema["embedding"], serde_json::json!("[128]f32"));
    }

    #[test]
    fn meta_after_wal_commit_rejects_gap() {
        let meta = NamespaceMeta {
            wal_commit_seq: 5,
            ..Default::default()
        };
        assert!(meta_after_wal_commit(&meta, 7).is_err());
        assert!(meta_after_wal_commit(&meta, 6).is_ok());
    }

    #[test]
    fn push_segment_id_dedupes_consecutive() {
        let mut ids = vec![1, 2];
        push_segment_id(&mut ids, 2);
        assert_eq!(ids, vec![1, 2]);
        push_segment_id(&mut ids, 3);
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn next_wal_seq_from_commit_point() {
        let meta = NamespaceMeta {
            wal_commit_seq: 42,
            ..Default::default()
        };
        assert_eq!(next_wal_seq(&meta), 43);
    }
}