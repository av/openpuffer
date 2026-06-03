//! In-process namespace view: cached document map + incremental WAL tail apply.
//!
//! Query loads replay WAL only for sequences after `last_applied_wal_seq`, not
//! `1..=N` on every request (see turbopuffer warm-path / strong consistency).

use crate::meta::NamespaceMeta;
use crate::models::Document;
use crate::namespace::{fetch_meta, load_legacy_docs, replay_wal_range};
use crate::wal::{apply_entry, WalEntry};
use anyhow::Result;
use aws_sdk_s3::Client;
use std::collections::HashMap;

/// Cached namespace state for queries (incrementally advanced from S3 WAL).
#[derive(Debug, Clone)]
pub struct NamespaceView {
    pub docs: HashMap<String, Document>,
    pub meta: NamespaceMeta,
    pub meta_etag: Option<String>,
    /// Last WAL sequence applied into `docs` (0 = empty / legacy-only).
    pub last_applied_wal_seq: u64,
}

impl NamespaceView {
    pub fn empty() -> Self {
        Self {
            docs: HashMap::new(),
            meta: NamespaceMeta::default(),
            meta_etag: None,
            last_applied_wal_seq: 0,
        }
    }

    /// Apply a committed WAL batch locally (after group-commit flush).
    pub fn apply_committed(&mut self, seq: u64, entry: &WalEntry) -> Result<()> {
        apply_entry(&mut self.docs, entry)?;
        self.last_applied_wal_seq = seq;
        if seq > self.meta.wal_commit_seq {
            self.meta.wal_commit_seq = seq;
        }
        Ok(())
    }

    /// Fetch new WAL segments since `last_applied_wal_seq` when meta advanced on S3.
    pub async fn catch_up(
        &mut self,
        client: &Client,
        bucket: &str,
        namespace: &str,
    ) -> Result<bool> {
        let Some((meta, etag)) = fetch_meta(client, bucket, namespace).await? else {
            return Ok(false);
        };

        if meta.wal_commit_seq <= self.last_applied_wal_seq {
            self.meta = meta;
            self.meta_etag = etag;
            return Ok(false);
        }

        let from = self.last_applied_wal_seq.saturating_add(1);
        replay_wal_range(
            client,
            bucket,
            namespace,
            &mut self.docs,
            from,
            meta.wal_commit_seq,
        )
        .await?;

        self.last_applied_wal_seq = meta.wal_commit_seq;
        self.meta = meta;
        self.meta_etag = etag;
        Ok(true)
    }

    /// Cold load: meta + full WAL replay, or legacy manifest path.
    pub async fn load(client: &Client, bucket: &str, namespace: &str) -> Result<Self> {
        if let Some((meta, etag)) = fetch_meta(client, bucket, namespace).await? {
            let mut docs = HashMap::new();
            let last = meta.wal_commit_seq;
            if last > 0 {
                replay_wal_range(client, bucket, namespace, &mut docs, 1, last).await?;
            }
            return Ok(Self {
                docs,
                meta,
                meta_etag: etag,
                last_applied_wal_seq: last,
            });
        }

        let docs = load_legacy_docs(client, bucket, namespace).await?;
        Ok(Self {
            docs,
            meta: NamespaceMeta::default(),
            meta_etag: None,
            last_applied_wal_seq: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Document;
    use crate::wal::WalEntry;
    use serde_json::json;

    #[test]
    fn incremental_apply_advances_seq() {
        let mut view = NamespaceView::empty();
        let e1 = WalEntry::from_write(
            vec![Document {
                id: "a".into(),
                attributes: [("k".into(), json!(1))].into(),
            }],
            vec![],
        )
        .unwrap();
        view.apply_committed(1, &e1).unwrap();
        assert_eq!(view.last_applied_wal_seq, 1);
        assert!(view.docs.contains_key("a"));

        let e2 = WalEntry::from_write(vec![], vec!["a".into()]).unwrap();
        view.apply_committed(2, &e2).unwrap();
        assert_eq!(view.last_applied_wal_seq, 2);
        assert!(!view.docs.contains_key("a"));
    }
}