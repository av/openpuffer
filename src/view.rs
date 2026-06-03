//! In-process namespace view: cached document map + incremental WAL tail apply.
//!
//! Query loads replay WAL only for sequences after `last_applied_wal_seq`, not
//! `1..=N` on every request (see turbopuffer warm-path / strong consistency).

use crate::meta::NamespaceMeta;
use crate::models::Document;
use crate::namespace::{fetch_meta, load_docs_at_wal_commit, read_wal_entry};
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
    /// Last WAL seq that touched each doc (for eventual consistency filtering).
    pub doc_last_wal_seq: HashMap<String, u64>,
}

impl NamespaceView {
    pub fn empty() -> Self {
        Self {
            docs: HashMap::new(),
            meta: NamespaceMeta::default(),
            meta_etag: None,
            last_applied_wal_seq: 0,
            doc_last_wal_seq: HashMap::new(),
        }
    }

    /// Doc map for queries; eventual consistency omits docs not yet in `index/`.
    pub fn docs_for_query(&self, skip_wal_tail: bool) -> HashMap<String, Document> {
        if !skip_wal_tail || self.meta.index_cursor >= self.meta.wal_commit_seq {
            return self.docs.clone();
        }
        self.docs
            .iter()
            .filter(|(id, _)| {
                self.doc_last_wal_seq
                    .get(*id)
                    .copied()
                    .unwrap_or(0)
                    <= self.meta.index_cursor
            })
            .map(|(id, doc)| (id.clone(), doc.clone()))
            .collect()
    }

    fn record_doc_wal_seq(&mut self, seq: u64, entry: &WalEntry) {
        for id in &entry.deletes {
            self.doc_last_wal_seq.remove(id);
        }
        if let Ok(docs) = entry.clone().into_documents() {
            for doc in docs {
                self.doc_last_wal_seq.insert(doc.id, seq);
            }
        }
        for doc in &entry.patches {
            self.doc_last_wal_seq.insert(doc.id.clone(), seq);
        }
    }

    /// Apply a committed WAL batch locally (after group-commit flush).
    pub fn apply_committed(&mut self, seq: u64, entry: &WalEntry) -> Result<()> {
        self.record_doc_wal_seq(seq, entry);
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
            self.doc_last_wal_seq =
                build_doc_last_wal_seq(client, bucket, namespace, &meta, Some(&self.docs)).await?;
            self.last_applied_wal_seq = last;
            self.meta = meta;
            self.meta_etag = etag;
            return Ok(true);
        }

        let from = self.last_applied_wal_seq.saturating_add(1);
        for seq in from..=meta.wal_commit_seq {
            let entry = read_wal_entry(client, bucket, namespace, seq).await?;
            self.record_doc_wal_seq(seq, &entry);
            apply_entry(&mut self.docs, &entry)?;
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
        let doc_last_wal_seq =
            build_doc_last_wal_seq(client, bucket, namespace, &meta, Some(&docs)).await?;
        Ok(Self {
            docs,
            meta,
            meta_etag: etag,
            last_applied_wal_seq: last,
            doc_last_wal_seq,
        })
    }

    /// Cold load without disk cache: batched meta + optional parallel WAL.
    ///
    /// When `include_wal` is false (`consistency: eventual`), only `meta.json` is fetched;
    /// `docs` stays empty and indexed segments supply query candidates.
    pub async fn load_cold_batched(
        client: &Client,
        bucket: &str,
        namespace: &str,
        include_wal: bool,
    ) -> Result<(Self, u32, u32)> {
        if !include_wal {
            let (meta, etag, wal_roundtrips, wal_s3_keys) =
                s3_batch::cold_load_meta_only(client, bucket, namespace).await?;
            if wal_roundtrips == 0 && etag.is_none() {
                return Ok((Self::empty(), 0, 0));
            }
            return Ok((
                Self {
                    docs: HashMap::new(),
                    meta,
                    meta_etag: etag,
                    last_applied_wal_seq: 0,
                    doc_last_wal_seq: HashMap::new(),
                },
                wal_roundtrips,
                wal_s3_keys,
            ));
        }

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
        let mut doc_last_wal_seq = HashMap::new();
        if meta.wal_snapshot_seq > 0 {
            for id in docs.keys() {
                doc_last_wal_seq.insert(id.clone(), meta.wal_snapshot_seq);
            }
        }
        if let Some(replay_from) = wal_replay_from(last_applied.max(meta.wal_snapshot_seq), last) {
            for seq in replay_from..=last {
                if let Some(bytes) = wal_bytes.get(&seq) {
                    if let Some(entry) =
                        decode_segment_with_policy(bytes, seq, policy)?
                    {
                        for id in &entry.deletes {
                            doc_last_wal_seq.remove(id);
                        }
                        if let Ok(batch) = entry.clone().into_documents() {
                            for doc in batch {
                                doc_last_wal_seq.insert(doc.id, seq);
                            }
                        }
                        for doc in &entry.patches {
                            doc_last_wal_seq.insert(doc.id.clone(), seq);
                        }
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
                doc_last_wal_seq,
            },
            wal_roundtrips,
            wal_s3_keys,
        ))
    }
}

async fn build_doc_last_wal_seq(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
    seed_docs: Option<&HashMap<String, Document>>,
) -> Result<HashMap<String, u64>> {
    let mut map = HashMap::new();
    if meta.wal_snapshot_seq > 0 {
        if let Some(docs) = seed_docs {
            for id in docs.keys() {
                map.insert(id.clone(), meta.wal_snapshot_seq);
            }
        }
    }
    let from = if meta.wal_snapshot_seq > 0 {
        meta.wal_snapshot_seq.saturating_add(1)
    } else {
        1
    };
    for seq in from..=meta.wal_commit_seq {
        let entry = read_wal_entry(client, bucket, namespace, seq).await?;
        for id in &entry.deletes {
            map.remove(id);
        }
        if let Ok(batch) = entry.clone().into_documents() {
            for doc in batch {
                map.insert(doc.id, seq);
            }
        }
        for doc in &entry.patches {
            map.insert(doc.id.clone(), seq);
        }
    }
    Ok(map)
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