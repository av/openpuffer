//! Namespace warm-cache: prefetch meta, index segments, WAL, and pin in-memory view.

use crate::cache::SegmentCache;
use crate::index::filter::FilterSegment;
use crate::index::fts::FtsSegment;
use crate::index::vector::{CentroidIndexL0, CentroidIndexL1, ClusterSegment};
use crate::meta::meta_key;
use crate::namespace::fetch_meta;
use crate::view::NamespaceView;
use crate::view_cache::ViewCache;
use crate::wal::{wal_key, WalSnapshot};
use crate::wal_compaction::wal_replay_from;
use anyhow::{anyhow, Result};
use aws_sdk_s3::Client;
use std::sync::Arc;
use std::time::Instant;

/// Maximum WAL segments to mirror into disk cache on warm (recent tail).
pub const WAL_WARM_MAX_SEGMENTS: u64 = 128;

/// Stats returned by `POST /v1/namespaces/{name}/warm`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WarmStats {
    pub status: &'static str,
    pub namespace: String,
    pub duration_ms: u64,
    pub pinned: bool,
    pub meta_loaded: bool,
    pub fts_segments: u32,
    pub filter_segments: u32,
    pub cluster_segments: u32,
    pub wal_segments: u32,
    pub s3_get_count: u64,
}

/// Collect S3 keys for index objects referenced by current metadata.
pub fn index_keys_for_warm(namespace: &str, meta: &crate::meta::NamespaceMeta) -> Vec<String> {
    let mut keys = Vec::new();
    if meta.fts_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FtsSegment::key(namespace, meta.fts_segment_id));
    }
    if meta.filter_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FilterSegment::key(namespace, meta.filter_segment_id));
    }
    if meta.vector_segment_id > 0 && meta.index_cursor > 0 && meta.dimensions > 0 {
        keys.push(CentroidIndexL0::key(namespace));
    }
    keys
}

/// WAL keys to warm: snapshot (if compacted) + retained tail segments only (not deleted history).
pub fn wal_keys_for_warm(namespace: &str, meta: &crate::meta::NamespaceMeta) -> Vec<String> {
    if meta.wal_commit_seq == 0 {
        return Vec::new();
    }
    let mut keys = Vec::new();
    if meta.wal_snapshot_seq > 0 {
        keys.push(WalSnapshot::key(namespace));
    }
    let replay_from = if meta.wal_snapshot_seq > 0 {
        wal_replay_from(meta.wal_snapshot_seq, meta.wal_commit_seq)
    } else if meta.wal_commit_seq > 0 {
        Some(1)
    } else {
        None
    };
    let Some(from) = replay_from else {
        return keys;
    };
    let span = (meta.wal_commit_seq - from + 1).min(WAL_WARM_MAX_SEGMENTS);
    let start = meta.wal_commit_seq.saturating_sub(span - 1).max(from);
    keys.extend((start..=meta.wal_commit_seq).map(|seq| wal_key(namespace, seq)));
    keys
}

/// Prefetch namespace artifacts into disk cache and pin [`NamespaceView`] in `view_cache`.
pub async fn warm_namespace(
    client: &Client,
    bucket: &str,
    namespace: &str,
    cache: &Arc<SegmentCache>,
    view_cache: &mut ViewCache,
) -> Result<WarmStats> {
    let started = Instant::now();
    let s3_gets_before = cache.s3_get_count();

    let Some((meta, _etag)) = fetch_meta(client, bucket, namespace).await? else {
        return Err(anyhow!("namespace not found"));
    };

    // Mirror meta.json into segment cache when disk cache is enabled.
    let meta_key = meta_key(namespace);
    let _ = cache.get_bytes(client, bucket, &meta_key).await?;

    let mut fts_segments = 0u32;
    let mut filter_segments = 0u32;
    let mut cluster_segments = 0u32;

    for key in index_keys_for_warm(namespace, &meta) {
        if key.ends_with("centroids-l0.bin") {
            if let Some(bytes) = cache.get_bytes(client, bucket, &key).await? {
                if let Ok(l0) = CentroidIndexL0::decode(&bytes) {
                    for coarse_id in 0..l0.num_coarse {
                        let l1_key = CentroidIndexL1::key(namespace, coarse_id);
                        let _ = cache.get_bytes(client, bucket, &l1_key).await?;
                    }
                    for fine_id in 0..l0.num_fine_total {
                        let ckey = ClusterSegment::key(namespace, fine_id);
                        if cache.get_bytes(client, bucket, &ckey).await?.is_some() {
                            cluster_segments += 1;
                        }
                    }
                }
            }
            continue;
        }
        if cache.get_bytes(client, bucket, &key).await?.is_some() {
            if key.contains("/index/fts-") {
                fts_segments += 1;
            } else if key.contains("/index/filter-") {
                filter_segments += 1;
            }
        }
    }

    let wal_keys = wal_keys_for_warm(namespace, &meta);
    let wal_segments = wal_keys.len() as u32;
    for key in &wal_keys {
        let _ = cache.get_bytes(client, bucket, key).await?;
    }

    let mut view = NamespaceView::load(client, bucket, namespace).await?;
    view.catch_up(client, bucket, namespace).await?;
    view_cache.insert(namespace.to_string(), view);

    let duration_ms = started.elapsed().as_millis() as u64;
    let s3_get_count = cache.s3_get_count().saturating_sub(s3_gets_before);

    Ok(WarmStats {
        status: "ok",
        namespace: namespace.to_string(),
        duration_ms,
        pinned: view_cache.contains(namespace),
        meta_loaded: true,
        fts_segments,
        filter_segments,
        cluster_segments,
        wal_segments,
        s3_get_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::NamespaceMeta;

    #[test]
    fn wal_keys_caps_recent_tail() {
        let meta = NamespaceMeta {
            wal_commit_seq: 200,
            ..Default::default()
        };
        let keys = wal_keys_for_warm("ns", &meta);
        assert_eq!(keys.len(), WAL_WARM_MAX_SEGMENTS as usize);
        assert!(keys.first().unwrap().contains("00000073"));
        assert!(keys.last().unwrap().contains("00000200"));
    }

    #[test]
    fn wal_keys_empty_when_no_commit() {
        assert!(wal_keys_for_warm("ns", &NamespaceMeta::default()).is_empty());
    }

    #[test]
    fn wal_keys_after_compaction_omit_deleted_segments() {
        let meta = NamespaceMeta {
            wal_commit_seq: 15,
            wal_snapshot_seq: 15,
            ..Default::default()
        };
        let keys = wal_keys_for_warm("ns", &meta);
        assert_eq!(keys.len(), 1);
        assert!(keys[0].ends_with("snapshot.bin"));
    }

    #[test]
    fn wal_keys_after_compaction_includes_tail() {
        let meta = NamespaceMeta {
            wal_commit_seq: 18,
            wal_snapshot_seq: 15,
            ..Default::default()
        };
        let keys = wal_keys_for_warm("ns", &meta);
        assert!(keys.iter().any(|k| k.ends_with("snapshot.bin")));
        assert!(keys.iter().any(|k| k.ends_with("00000016.bin")));
        assert!(keys.iter().any(|k| k.ends_with("00000018.bin")));
        assert!(!keys.iter().any(|k| k.ends_with("00000001.bin")));
    }

    #[test]
    fn index_keys_include_fts_filter_centroids() {
        let meta = NamespaceMeta {
            index_cursor: 5,
            fts_segment_id: 5,
            filter_segment_id: 5,
            vector_segment_id: 5,
            dimensions: 3,
            wal_commit_seq: 5,
            ..Default::default()
        };
        let keys = index_keys_for_warm("my-ns", &meta);
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().any(|k| k.contains("fts-")));
        assert!(keys.iter().any(|k| k.contains("filter-")));
        assert!(keys.iter().any(|k| k.ends_with("centroids-l0.bin")));
    }
}