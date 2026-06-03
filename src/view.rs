//! In-process namespace view: cached document map + incremental WAL tail apply.
//!
//! Query loads replay WAL only for sequences after `last_applied_wal_seq`, not
//! `1..=N` on every request (see turbopuffer warm-path / strong consistency).

use crate::meta::NamespaceMeta;
use crate::models::Document;
use crate::namespace::{fetch_meta, load_docs_at_wal_commit, replay_wal_range};
use crate::wal_compaction::wal_replay_from;
use crate::s3_batch;
use crate::wal::{
    apply_entry, decode_segment_with_policy, decode_snapshot, WalCorruptPolicy, WalEntry,
};
use anyhow::Result;
use aws_sdk_s3::Client;
use std::collections::HashMap;

/// Cached namespace state for queries (incrementally advanced from S3 WAL).
#[derive(Debug, Clone)]
pub struct NamespaceView {
    pub docs: HashMap<String, Document>,
    pub meta: NamespaceMeta,
    pub meta_etag: Option<String>,
    /// Last WAL sequence applied into `docs` (0 = empty namespace).
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

        // WAL compaction may delete segments between our last apply and `wal_snapshot_seq`.
        if meta.wal_snapshot_seq > self.last_applied_wal_seq {
            let (docs, last) =
                load_docs_at_wal_commit(client, bucket, namespace, &meta).await?;
            self.docs = docs;
            self.last_applied_wal_seq = last;
            self.meta = meta;
            self.meta_etag = etag;
            return Ok(true);
        }

        let from = self.last_applied_wal_seq.saturating_add(1);
        if from <= meta.wal_commit_seq {
            replay_wal_range(
                client,
                bucket,
                namespace,
                &mut self.docs,
                from,
                meta.wal_commit_seq,
            )
            .await?;
        }

        self.last_applied_wal_seq = meta.wal_commit_seq;
        self.meta = meta;
        self.meta_etag = etag;
        Ok(true)
    }

    /// Cold load: `meta.json` + full WAL replay. No `meta.json` → empty namespace.
    pub async fn load(client: &Client, bucket: &str, namespace: &str) -> Result<Self> {
        let Some((meta, etag)) = fetch_meta(client, bucket, namespace).await? else {
            return Ok(Self::empty());
        };
        let (docs, last) =
            load_docs_at_wal_commit(client, bucket, namespace, &meta).await?;
        Ok(Self {
            docs,
            meta,
            meta_etag: etag,
            last_applied_wal_seq: last,
        })
    }

    /// Cold load without disk cache: batched meta + parallel WAL (`storage_roundtrips` for WAL only).
    pub async fn load_cold_batched(
        client: &Client,
        bucket: &str,
        namespace: &str,
    ) -> Result<(Self, u32, u32)> {
        let (meta, etag, wal_bytes, wal_roundtrips, wal_s3_keys) =
            s3_batch::cold_load_meta_and_wal(client, bucket, namespace).await?;
        if wal_roundtrips == 0 && etag.is_none() {
            return Ok((Self::empty(), 0, 0));
        }
        let mut docs = HashMap::new();
        let mut last_applied = 0u64;
        if let Some(snap_bytes) = wal_bytes.get(&0) {
            let snap = decode_snapshot(snap_bytes)?;
            last_applied = snap.seq;
            docs = snap.into_docs();
        }
        let last = meta.wal_commit_seq;
        let policy = WalCorruptPolicy::current();
        if let Some(replay_from) = wal_replay_from(last_applied, last) {
            for seq in replay_from..=last {
                if let Some(bytes) = wal_bytes.get(&seq) {
                    if let Some(entry) =
                        decode_segment_with_policy(bytes, seq, policy)?
                    {
                        apply_entry(&mut docs, &entry)?;
                    }
                }
            }
        }
        Ok((
            Self {
                docs,
                meta,
                meta_etag: etag,
                last_applied_wal_seq: last,
            },
            wal_roundtrips,
            wal_s3_keys,
        ))
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
            vec![],
        )
        .unwrap();
        view.apply_committed(1, &e1).unwrap();
        assert_eq!(view.last_applied_wal_seq, 1);
        assert!(view.docs.contains_key("a"));

        let e2 = WalEntry::from_write(vec![], vec![], vec!["a".into()]).unwrap();
        view.apply_committed(2, &e2).unwrap();
        assert_eq!(view.last_applied_wal_seq, 2);
        assert!(!view.docs.contains_key("a"));
    }
}