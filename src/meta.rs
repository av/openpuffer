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
    /// Last committed WAL file sequence (`wal/{seq:08}.bin`).
    pub wal_commit_seq: u64,
    #[serde(default)]
    pub schema: Value,
    #[serde(default)]
    pub distance_metric: DistanceMetric,
}

impl Default for NamespaceMeta {
    fn default() -> Self {
        Self {
            index_cursor: 0,
            wal_commit_seq: 0,
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

/// Build updated metadata after appending WAL object `seq` (CAS payload).
pub fn meta_after_wal_commit(meta: &NamespaceMeta, seq: u64) -> Result<NamespaceMeta> {
    if seq != meta.wal_commit_seq.saturating_add(1) {
        return Err(anyhow!(
            "wal seq {seq} does not follow commit point {}",
            meta.wal_commit_seq
        ));
    }
    let mut next = meta.clone();
    next.wal_commit_seq = seq;
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_json_roundtrip() {
        let meta = NamespaceMeta {
            index_cursor: 3,
            wal_commit_seq: 10,
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
    fn meta_after_wal_commit_rejects_gap() {
        let meta = NamespaceMeta {
            wal_commit_seq: 5,
            ..Default::default()
        };
        assert!(meta_after_wal_commit(&meta, 7).is_err());
        assert!(meta_after_wal_commit(&meta, 6).is_ok());
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