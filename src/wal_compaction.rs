//! WAL segment compaction after the indexer catches up (turbopuffer storage hygiene).
//!
//! When `index_cursor == wal_commit_seq` and commit seq exceeds a threshold, we:
//! 1. Materialize the namespace doc map at `index_cursor` (WAL replay or prior snapshot + tail).
//! 2. PUT `wal/snapshot.bin` so cold load / export do not need deleted segments.
//! 3. DELETE `wal/{seq:08}.bin` for `seq <= index_cursor - keep_k` (retain last N for debugging).

use crate::cache::SegmentCache;
use crate::commit_lock::namespace_commit_lock;
use crate::meta::{meta_key, NamespaceMeta, META_RETRIES};
use crate::namespace::{docs_at_index_cursor, fetch_meta};
use crate::wal::{encode_snapshot, wal_key, WalSnapshot};

use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::sync::Arc;
use std::time::Duration;

/// Run compaction only after this many WAL commits (avoids churn on tiny namespaces).
pub const WAL_COMPACTION_THRESHOLD: u64 = 10;

/// Retain this many trailing WAL segment files after compaction (debugging / tail consistency).
pub const WAL_COMPACT_KEEP: u64 = 3;

/// True when indexer is caught up and WAL history is long enough to compact.
pub fn should_compact_wal(meta: &NamespaceMeta) -> bool {
    meta.index_cursor >= meta.wal_commit_seq
        && meta.wal_commit_seq > WAL_COMPACTION_THRESHOLD
        && meta.index_cursor > WAL_COMPACT_KEEP
}

/// True when compaction should run and `meta` has not yet recorded the latest snapshot.
pub fn needs_wal_compaction(meta: &NamespaceMeta) -> bool {
    should_compact_wal(meta) && meta.wal_snapshot_seq < meta.index_cursor
}

/// WAL sequence numbers safe to delete once fully indexed (`1..=index_cursor - keep_k`).
pub fn wal_seqs_to_delete(index_cursor: u64, keep_k: u64) -> Vec<u64> {
    if index_cursor <= keep_k {
        return Vec::new();
    }
    let delete_through = index_cursor.saturating_sub(keep_k);
    (1..=delete_through).collect()
}

/// First WAL seq to fetch on cold load when a snapshot exists at `snapshot_seq`.
pub fn wal_replay_from(snapshot_seq: u64, wal_commit_seq: u64) -> Option<u64> {
    if wal_commit_seq == 0 {
        return None;
    }
    let from = snapshot_seq.saturating_add(1);
    if from > wal_commit_seq {
        None
    } else {
        Some(from.max(1))
    }
}

/// Compact indexed WAL segments for `namespace` if caught up. Returns count of objects deleted.
pub async fn maybe_compact_wal(
    client: &Client,
    bucket: &str,
    namespace: &str,
    cache: &Arc<SegmentCache>,
) -> Result<u32> {
    let Some((meta, _)) = fetch_meta(client, bucket, namespace).await? else {
        return Ok(0);
    };
    if !needs_wal_compaction(&meta) {
        return Ok(0);
    }

    let commit_lock = namespace_commit_lock(namespace).await;
    let _guard = commit_lock.lock().await;

    for attempt in 0..META_RETRIES {
        let Some((meta, etag)) = fetch_meta(client, bucket, namespace).await? else {
            return Ok(0);
        };
        if !needs_wal_compaction(&meta) {
            return Ok(0);
        }

        let cursor = meta.index_cursor;
        let docs = docs_at_index_cursor(client, bucket, namespace, cursor).await?;
        let snapshot = WalSnapshot::from_docs(cursor, &docs);
        let snap_body = encode_snapshot(&snapshot)?;
        let snap_key = WalSnapshot::key(namespace);
        let snap_resp = client
            .put_object()
            .bucket(bucket)
            .key(&snap_key)
            .body(ByteStream::from(snap_body.clone()))
            .send()
            .await
            .context("put wal snapshot")?;
        cache.populate_after_put(bucket, &snap_key, &snap_body, snap_resp.e_tag());

        let mut next_meta = meta.clone();
        next_meta.wal_snapshot_seq = cursor;

        let meta_body = serde_json::to_vec(&next_meta)?;
        let mkey = meta_key(namespace);
        let mut put = client
            .put_object()
            .bucket(bucket)
            .key(&mkey)
            .body(ByteStream::from(meta_body));
        if let Some(etag) = &etag {
            put = put.if_match(etag);
        } else {
            put = put.if_none_match("*");
        }

        match put.send().await {
            Ok(_) => {
                let to_delete = wal_seqs_to_delete(cursor, WAL_COMPACT_KEEP);
                let mut deleted = 0u32;
                for seq in to_delete {
                    let key = wal_key(namespace, seq);
                    match client
                        .delete_object()
                        .bucket(bucket)
                        .key(&key)
                        .send()
                        .await
                    {
                        Ok(_) => {
                            cache.invalidate_key(bucket, &key);
                            deleted += 1;
                        }
                        Err(e) => {
                            let service = e.into_service_error();
                            let code = service.meta().code();
                            if code != Some("NoSuchKey") && code != Some("NotFound") {
                                tracing::warn!(
                                    "wal compact delete {key} for {namespace}: {service}"
                                );
                            }
                        }
                    }
                }
                tracing::debug!(
                    "wal compact {namespace}: snapshot_seq={cursor} deleted_segments={deleted}"
                );
                return Ok(deleted);
            }
            Err(e) => {
                let service = e.into_service_error();
                if service.meta().code() == Some("PreconditionFailed")
                    && attempt + 1 < META_RETRIES
                {
                    tokio::time::sleep(Duration::from_millis(50 * (attempt as u64 + 1))).await;
                    continue;
                }
                return Err(anyhow!("put meta after wal compact: {service}"));
            }
        }
    }
    Err(anyhow!("meta CAS failed after wal compaction retries"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::NamespaceMeta;

    #[test]
    fn should_compact_when_caught_up_past_threshold() {
        let meta = NamespaceMeta {
            index_cursor: 15,
            wal_commit_seq: 15,
            ..Default::default()
        };
        assert!(should_compact_wal(&meta));
    }

    #[test]
    fn should_not_compact_when_behind() {
        let meta = NamespaceMeta {
            index_cursor: 10,
            wal_commit_seq: 15,
            ..Default::default()
        };
        assert!(!should_compact_wal(&meta));
    }

    #[test]
    fn should_not_compact_below_threshold() {
        let meta = NamespaceMeta {
            index_cursor: 8,
            wal_commit_seq: 8,
            ..Default::default()
        };
        assert!(!should_compact_wal(&meta));
    }

    #[test]
    fn wal_seqs_to_delete_keeps_last_k() {
        assert_eq!(wal_seqs_to_delete(15, 3), (1..=12).collect::<Vec<_>>());
        assert!(wal_seqs_to_delete(3, 3).is_empty());
    }

    #[test]
    fn wal_replay_from_after_snapshot() {
        assert_eq!(wal_replay_from(15, 15), None);
        assert_eq!(wal_replay_from(15, 18), Some(16));
        assert_eq!(wal_replay_from(0, 5), Some(1));
    }
}