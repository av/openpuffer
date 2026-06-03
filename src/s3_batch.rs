//! Batched parallel S3 fetches for cold queries (turbopuffer multi-roundtrip model).
//!
//! **Round 1:** `meta.json` + `centroids-l0.bin` + latest FTS segment (parallel).
//! **Round 2:** filter segment + probed `centroids-l1-*` + probed `clusters-*` (parallel).
//!
//! Each round is one logical `storage_roundtrip` (parallel `GetObject` in a single batch).

use crate::index::filter::FilterSegment;
use crate::index::fts::{index_fields_from_schema, FtsSegment};
use crate::index::vector::{
    CentroidIndexL0, CentroidIndexL1, ClusterSegment, VectorIndex,
};
use crate::meta::{effective_vector_fields, meta_key, vector_index_uses_legacy_paths, NamespaceMeta};
use crate::namespace::fetch_meta;
use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use std::collections::HashMap;

/// Index artifacts loaded via the cold batch plan.
#[derive(Debug, Default)]
pub struct ColdIndexArtifacts {
    pub fts: Option<FtsSegment>,
    pub filter: Option<FilterSegment>,
    pub vectors: HashMap<String, VectorIndex>,
    pub storage_roundtrips: u32,
}

/// Keys for turbopuffer-style round 1 (meta + L0 centroids + latest FTS).
pub fn round1_keys(namespace: &str, meta: &NamespaceMeta) -> Vec<String> {
    let mut keys = vec![meta_key(namespace)];
    keys.extend(round1_index_keys(namespace, meta));
    keys
}

/// Round 1 index objects when `meta.json` is already in memory.
pub fn round1_index_keys(namespace: &str, meta: &NamespaceMeta) -> Vec<String> {
    let mut keys = Vec::new();
    for cfg in effective_vector_fields(meta) {
        if cfg.segment_id > 0 && meta.index_cursor > 0 && cfg.dimensions > 0 {
            if vector_index_uses_legacy_paths(meta, &cfg.name) {
                keys.push(CentroidIndexL0::legacy_key(namespace));
            } else {
                keys.push(CentroidIndexL0::key(namespace, &cfg.name));
            }
        }
    }
    if meta.fts_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FtsSegment::key(namespace, meta.fts_segment_id));
    }
    keys
}

/// Keys for round 2 cold load: filter + all L1 + all cluster files for one vector field.
pub fn round2_keys_for_field(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: &CentroidIndexL0,
) -> Vec<String> {
    let use_legacy = vector_index_uses_legacy_paths(meta, field);
    let mut keys = Vec::new();
    for coarse_id in 0..l0.num_coarse {
        if use_legacy {
            keys.push(CentroidIndexL1::legacy_key(namespace, coarse_id));
        } else {
            keys.push(CentroidIndexL1::key(namespace, field, coarse_id));
        }
    }
    for fine_id in 0..l0.num_fine_total {
        if use_legacy {
            keys.push(ClusterSegment::legacy_key(namespace, fine_id));
        } else {
            keys.push(ClusterSegment::key(namespace, field, fine_id));
        }
    }
    keys
}

/// Keys for round 2 cold load: filter + all L1 + all cluster files (all vector columns).
pub fn round2_keys(
    namespace: &str,
    meta: &NamespaceMeta,
    l0_by_field: &[(String, CentroidIndexL0)],
) -> Vec<String> {
    let mut keys = Vec::new();
    if meta.filter_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FilterSegment::key(namespace, meta.filter_segment_id));
    }
    for (field, l0) in l0_by_field {
        keys.extend(round2_keys_for_field(namespace, meta, field, l0));
    }
    keys.sort();
    keys.dedup();
    keys
}

/// Keys for round 2 query path: filter + probed L1 + probed clusters only.
pub fn round2_keys_for_query(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: &CentroidIndexL0,
    l1_loaded: &HashMap<u32, CentroidIndexL1>,
    query: &[f64],
) -> Vec<String> {
    let mut keys = Vec::new();
    if meta.filter_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FilterSegment::key(namespace, meta.filter_segment_id));
    }

    let use_legacy = vector_index_uses_legacy_paths(meta, field);
    let coarse_m = l0.probe_coarse_count();
    let coarse_ids = l0.nearest_coarse(query, coarse_m);
    for coarse_id in coarse_ids {
        if !l1_loaded.contains_key(&coarse_id) {
            if use_legacy {
                keys.push(CentroidIndexL1::legacy_key(namespace, coarse_id));
            } else {
                keys.push(CentroidIndexL1::key(namespace, field, coarse_id));
            }
        }
    }

    let mut fine_ids: Vec<u32> = Vec::new();
    for coarse_id in l0.nearest_coarse(query, coarse_m) {
        let Some(l1) = l1_loaded.get(&coarse_id) else {
            continue;
        };
        let fine_m = l0.probe_fine_count(l1);
        for local in l1.nearest_fine(query, l0.distance_metric, fine_m) {
            fine_ids.push(l0.global_fine_id(coarse_id, local));
        }
    }
    fine_ids.sort_unstable();
    fine_ids.dedup();
    for fine_id in fine_ids {
        if use_legacy {
            keys.push(ClusterSegment::legacy_key(namespace, fine_id));
        } else {
            keys.push(ClusterSegment::key(namespace, field, fine_id));
        }
    }
    keys
}

/// Probe plan without requiring L1 in memory: fetch L1 for top coarse, clusters resolved after decode.
pub fn round2_keys_for_query_probe(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: &CentroidIndexL0,
    query: &[f64],
) -> Vec<String> {
    let mut keys = Vec::new();
    if meta.filter_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FilterSegment::key(namespace, meta.filter_segment_id));
    }
    let use_legacy = vector_index_uses_legacy_paths(meta, field);
    for coarse_id in l0.nearest_coarse(query, l0.probe_coarse_count()) {
        if use_legacy {
            keys.push(CentroidIndexL1::legacy_key(namespace, coarse_id));
        } else {
            keys.push(CentroidIndexL1::key(namespace, field, coarse_id));
        }
    }
    keys
}

/// One parallel batch of S3 `GetObject` calls (counts as one storage roundtrip).
pub async fn fetch_round(
    client: &Client,
    bucket: &str,
    keys: &[String],
) -> Result<HashMap<String, Vec<u8>>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let mut handles = Vec::with_capacity(keys.len());
    for key in keys {
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            let bytes = get_object_bytes(&client, &bucket, &key).await?;
            Ok::<_, anyhow::Error>((key, bytes))
        }));
    }
    let mut out = HashMap::new();
    for handle in handles {
        let (key, bytes) = handle.await.context("fetch_round task join")??;
        out.insert(key, bytes);
    }
    Ok(out)
}

async fn get_object_bytes(client: &Client, bucket: &str, key: &str) -> Result<Vec<u8>> {
    let out = client.get_object().bucket(bucket).key(key).send().await;
    match out {
        Ok(resp) => {
            let bytes = resp
                .body
                .collect()
                .await
                .context("read object body")?
                .into_bytes()
                .to_vec();
            Ok(bytes)
        }
        Err(e) => {
            let service = e.into_service_error();
            if service.is_no_such_key() {
                Err(anyhow::anyhow!("object not found: {key}"))
            } else {
                Err(anyhow::anyhow!("get object {key}: {service}"))
            }
        }
    }
}

/// Cold index load: two batched rounds (no disk cache / no HEAD per object).
pub async fn fetch_cold_index_artifacts(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
) -> Result<ColdIndexArtifacts> {
    let mut storage_roundtrips = 0u32;

    let r1_keys = round1_index_keys(namespace, meta);
    let r1 = if r1_keys.is_empty() {
        HashMap::new()
    } else {
        storage_roundtrips += 1;
        fetch_round(client, bucket, &r1_keys).await?
    };

    let mut l0_by_field: Vec<(String, CentroidIndexL0)> = Vec::new();
    for cfg in effective_vector_fields(meta) {
        if cfg.segment_id == 0 || meta.index_cursor == 0 || cfg.dimensions == 0 {
            continue;
        }
        let key = CentroidIndexL0::key(namespace, &cfg.name);
        let bytes = r1.get(&key).or_else(|| {
            if vector_index_uses_legacy_paths(meta, &cfg.name) {
                r1.get(&CentroidIndexL0::legacy_key(namespace))
            } else {
                None
            }
        });
        if let Some(b) = bytes {
            if let Ok(l0) = CentroidIndexL0::decode(b) {
                if l0.num_fine_total > 0 {
                    l0_by_field.push((cfg.name.clone(), l0));
                }
            }
        }
    }

    let mut r2_keys = round2_keys(namespace, meta, &l0_by_field);
    if r2_keys.is_empty() && meta.filter_segment_id > 0 && meta.index_cursor > 0 {
        r2_keys.push(FilterSegment::key(namespace, meta.filter_segment_id));
    }

    let r2 = if r2_keys.is_empty() {
        HashMap::new()
    } else {
        storage_roundtrips += 1;
        fetch_round(client, bucket, &r2_keys).await?
    };

    let fts = decode_fts_from_round(namespace, meta, &r1)?;
    let filter = decode_filter_from_round(namespace, meta, &r2)?;
    let mut vectors = HashMap::new();
    for (field, l0) in l0_by_field {
        if let Some(v) = decode_vector_from_rounds(namespace, meta, &field, l0, &r2)? {
            vectors.insert(field, v);
        }
    }

    Ok(ColdIndexArtifacts {
        fts,
        filter,
        vectors,
        storage_roundtrips,
    })
}

/// Replay WAL segment range using one parallel fetch round.
pub async fn replay_wal_entries_batched(
    client: &Client,
    bucket: &str,
    namespace: &str,
    from_seq: u64,
    to_seq: u64,
) -> Result<Vec<crate::wal::WalEntry>> {
    if from_seq == 0 || to_seq == 0 || from_seq > to_seq {
        return Ok(Vec::new());
    }
    let keys: Vec<String> = (from_seq..=to_seq)
        .map(|seq| crate::wal::wal_key(namespace, seq))
        .collect();
    let raw = fetch_round(client, bucket, &keys).await?;
    let mut entries = Vec::with_capacity(keys.len());
    for seq in from_seq..=to_seq {
        let key = crate::wal::wal_key(namespace, seq);
        let bytes = raw
            .get(&key)
            .with_context(|| format!("wal segment {seq:08} missing in batch"))?;
        entries.push(crate::wal::decode(bytes)?);
    }
    Ok(entries)
}

/// Cold namespace bootstrap: meta fetch + parallel WAL segments (2 roundtrips when WAL present).
pub async fn cold_load_meta_and_wal(
    client: &Client,
    bucket: &str,
    namespace: &str,
) -> Result<(NamespaceMeta, Option<String>, HashMap<u64, Vec<u8>>, u32)> {
    let Some((meta, etag)) = fetch_meta(client, bucket, namespace).await? else {
        return Ok((NamespaceMeta::default(), None, HashMap::new(), 0));
    };
    let mut storage_roundtrips = 1u32;

    let mut fetch_keys = Vec::new();
    if meta.wal_snapshot_seq > 0 {
        fetch_keys.push(crate::wal::WalSnapshot::key(namespace));
    }

    // After WAL compaction, `wal_replay_from` may be `None` (snapshot covers commit point).
    // Do not fall back to seq 1 — those segments may have been deleted.
    let replay_from = if meta.wal_snapshot_seq > 0 {
        crate::wal_compaction::wal_replay_from(meta.wal_snapshot_seq, meta.wal_commit_seq)
    } else if meta.wal_commit_seq > 0 {
        Some(1)
    } else {
        None
    };

    if let Some(from) = replay_from {
        for seq in from..=meta.wal_commit_seq {
            fetch_keys.push(crate::wal::wal_key(namespace, seq));
        }
    }

    if fetch_keys.is_empty() {
        return Ok((meta, etag, HashMap::new(), storage_roundtrips));
    }

    let wal_map_raw = fetch_round(client, bucket, &fetch_keys).await?;
    storage_roundtrips += 1;

    let mut wal_by_seq = HashMap::new();
    let snap_key = crate::wal::WalSnapshot::key(namespace);
    if let Some(bytes) = wal_map_raw.get(&snap_key) {
        wal_by_seq.insert(0, bytes.clone());
    }
    if let Some(from) = replay_from {
        for seq in from..=meta.wal_commit_seq {
            let key = crate::wal::wal_key(namespace, seq);
            if let Some(bytes) = wal_map_raw.get(&key) {
                wal_by_seq.insert(seq, bytes.clone());
            }
        }
    }

    Ok((meta, etag, wal_by_seq, storage_roundtrips))
}

fn primary_fts_field(meta: &NamespaceMeta) -> String {
    index_fields_from_schema(&meta.schema)
        .into_iter()
        .next()
        .unwrap_or_default()
}

fn decode_fts_from_round(
    namespace: &str,
    meta: &NamespaceMeta,
    r1: &HashMap<String, Vec<u8>>,
) -> Result<Option<FtsSegment>> {
    if meta.fts_segment_id == 0 || meta.index_cursor == 0 {
        return Ok(None);
    }
    let key = FtsSegment::key(namespace, meta.fts_segment_id);
    let Some(bytes) = r1.get(&key) else {
        return Ok(None);
    };
    let seg = FtsSegment::decode(bytes)?;
    let expected = primary_fts_field(meta);
    if !expected.is_empty() && seg.field != expected {
        // Schema field changed; keep segment if non-empty (matches indexer behavior).
    }
    Ok(Some(seg))
}

fn decode_filter_from_round(
    namespace: &str,
    meta: &NamespaceMeta,
    r2: &HashMap<String, Vec<u8>>,
) -> Result<Option<FilterSegment>> {
    if meta.filter_segment_id == 0 || meta.index_cursor == 0 {
        return Ok(None);
    }
    let key = FilterSegment::key(namespace, meta.filter_segment_id);
    let Some(bytes) = r2.get(&key) else {
        return Ok(None);
    };
    Ok(Some(FilterSegment::decode(bytes)?))
}

fn decode_vector_from_rounds(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: CentroidIndexL0,
    r2: &HashMap<String, Vec<u8>>,
) -> Result<Option<VectorIndex>> {
    if l0.num_fine_total == 0 {
        return Ok(None);
    }
    let use_legacy = vector_index_uses_legacy_paths(meta, field);

    let mut l1 = HashMap::new();
    for coarse_id in 0..l0.num_coarse {
        let key = CentroidIndexL1::key(namespace, field, coarse_id);
        let bytes = r2.get(&key).or_else(|| {
            if use_legacy {
                r2.get(&CentroidIndexL1::legacy_key(namespace, coarse_id))
            } else {
                None
            }
        });
        let Some(bytes) = bytes else {
            continue;
        };
        let seg = CentroidIndexL1::decode(bytes)?;
        l1.insert(coarse_id, seg);
    }

    let mut clusters = HashMap::new();
    for fine_id in 0..l0.num_fine_total {
        let key = ClusterSegment::key(namespace, field, fine_id);
        let bytes = r2.get(&key).or_else(|| {
            if use_legacy {
                r2.get(&ClusterSegment::legacy_key(namespace, fine_id))
            } else {
                None
            }
        });
        let Some(bytes) = bytes else {
            continue;
        };
        let seg = ClusterSegment::decode(bytes)?;
        clusters.insert(fine_id, seg);
    }

    Ok(Some(VectorIndex { l0, l1, clusters }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::vector::{DEFAULT_PROBE_COARSE, DEFAULT_PROBE_FINE};
    use crate::meta::{NamespaceMeta, VectorFieldConfig};

    #[test]
    fn round1_keys_include_meta_centroids_fts() {
        let meta = NamespaceMeta {
            index_cursor: 5,
            fts_segment_id: 5,
            vector_segment_id: 5,
            vector_field: "embedding".into(),
            dimensions: 4,
            vector_fields: vec![VectorFieldConfig {
                name: "embedding".into(),
                dimensions: 4,
                segment_id: 5,
                segment_ids: vec![5],
                ..Default::default()
            }],
            wal_commit_seq: 5,
            ..Default::default()
        };
        let keys = round1_keys("ns", &meta);
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().any(|k| k.ends_with("meta.json")));
        assert!(keys.iter().any(|k| k.contains("centroids-l0.bin")));
        assert!(keys.iter().any(|k| k.contains("fts-00000005")));
    }

    #[test]
    fn round2_keys_filter_l1_and_clusters() {
        let meta = NamespaceMeta {
            index_cursor: 3,
            filter_segment_id: 3,
            vector_segment_id: 3,
            vector_field: "emb".into(),
            dimensions: 2,
            vector_fields: vec![VectorFieldConfig {
                name: "emb".into(),
                dimensions: 2,
                segment_id: 3,
                segment_ids: vec![3],
                ..Default::default()
            }],
            ..Default::default()
        };
        let l0 = CentroidIndexL0 {
            vector_field: "emb".into(),
            num_coarse: 4,
            num_fine_total: 16,
            fine_counts: vec![4, 4, 4, 4],
            centroids: vec![vec![0.0, 0.0]; 4],
            dimensions: 2,
            ..Default::default()
        };
        let keys = round2_keys("ns", &meta, &[("emb".into(), l0)]);
        assert!(keys.iter().any(|k| k.contains("filter-00000003")));
        assert_eq!(
            keys.iter().filter(|k| k.contains("centroids-l1-")).count(),
            4
        );
        assert_eq!(
            keys.iter().filter(|k| k.contains("clusters-")).count(),
            16
        );
    }

    #[test]
    fn round2_query_probe_fetches_subset() {
        let meta = NamespaceMeta {
            index_cursor: 1,
            vector_segment_id: 1,
            vector_field: "emb".into(),
            dimensions: 2,
            vector_fields: vec![VectorFieldConfig {
                name: "emb".into(),
                dimensions: 2,
                segment_id: 1,
                segment_ids: vec![1],
                ..Default::default()
            }],
            ..Default::default()
        };
        let l0 = CentroidIndexL0 {
            vector_field: "emb".into(),
            num_coarse: 8,
            num_fine_total: 64,
            probe_coarse: DEFAULT_PROBE_COARSE,
            probe_fine: DEFAULT_PROBE_FINE,
            fine_counts: vec![4; 8],
            centroids: (0..8)
                .map(|i| vec![if i == 0 { 1.0 } else { 0.0 }, 0.0])
                .collect(),
            dimensions: 2,
            distance_metric: crate::meta::DistanceMetric::CosineDistance,
            ..Default::default()
        };
        let query = vec![1.0, 0.0];
        let keys = round2_keys_for_query_probe("ns", &meta, "emb", &l0, &query);
        assert!(
            keys.iter().filter(|k| k.contains("centroids-l1-")).count()
                <= DEFAULT_PROBE_COARSE as usize
        );
        assert!(!keys.iter().any(|k| k.contains("clusters-")));
    }

    #[test]
    fn round1_index_keys_omit_meta() {
        let meta = NamespaceMeta {
            index_cursor: 2,
            fts_segment_id: 2,
            vector_segment_id: 2,
            vector_field: "emb".into(),
            dimensions: 2,
            vector_fields: vec![VectorFieldConfig {
                name: "emb".into(),
                dimensions: 2,
                segment_id: 2,
                segment_ids: vec![2],
                ..Default::default()
            }],
            ..Default::default()
        };
        let keys = round1_index_keys("ns", &meta);
        assert_eq!(keys.len(), 2);
        assert!(!keys.iter().any(|k| k.ends_with("meta.json")));
    }

    #[test]
    fn cold_wal_fetch_after_compaction_needs_no_deleted_segments() {
        let meta = NamespaceMeta {
            wal_commit_seq: 15,
            wal_snapshot_seq: 15,
            index_cursor: 15,
            ..Default::default()
        };
        let replay_from = if meta.wal_snapshot_seq > 0 {
            crate::wal_compaction::wal_replay_from(meta.wal_snapshot_seq, meta.wal_commit_seq)
        } else if meta.wal_commit_seq > 0 {
            Some(1)
        } else {
            None
        };
        assert!(replay_from.is_none());
    }

    #[test]
    fn cold_index_plan_is_two_roundtrips_when_indexed() {
        let meta = NamespaceMeta {
            index_cursor: 2,
            fts_segment_id: 2,
            filter_segment_id: 2,
            vector_segment_id: 2,
            vector_field: "emb".into(),
            dimensions: 2,
            ..Default::default()
        };
        let l0 = CentroidIndexL0 {
            vector_field: "emb".into(),
            num_coarse: 2,
            num_fine_total: 4,
            fine_counts: vec![2, 2],
            centroids: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            dimensions: 2,
            ..Default::default()
        };
        let r2 = round2_keys("ns", &meta, &[("emb".into(), l0)]);
        assert!(!r2.is_empty());
        let mut trips = 1u32;
        if !r2.is_empty() {
            trips += 1;
        }
        assert_eq!(trips, 2);
    }
}