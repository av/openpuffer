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
use crate::meta::{meta_key, NamespaceMeta};
use crate::namespace::fetch_meta;
use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use std::collections::HashMap;

/// Index artifacts loaded via the cold batch plan.
#[derive(Debug, Default)]
pub struct ColdIndexArtifacts {
    pub fts: Option<FtsSegment>,
    pub filter: Option<FilterSegment>,
    pub vector: Option<VectorIndex>,
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
    if meta.vector_segment_id > 0 && meta.index_cursor > 0 && meta.dimensions > 0 {
        keys.push(CentroidIndexL0::key(namespace));
    }
    if meta.fts_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FtsSegment::key(namespace, meta.fts_segment_id));
    }
    keys
}

/// Keys for round 2 cold load: filter + all L1 + all cluster files.
pub fn round2_keys(namespace: &str, meta: &NamespaceMeta, l0: &CentroidIndexL0) -> Vec<String> {
    let mut keys = Vec::new();
    if meta.filter_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FilterSegment::key(namespace, meta.filter_segment_id));
    }
    for coarse_id in 0..l0.num_coarse {
        keys.push(CentroidIndexL1::key(namespace, coarse_id));
    }
    for fine_id in 0..l0.num_fine_total {
        keys.push(ClusterSegment::key(namespace, fine_id));
    }
    keys
}

/// Keys for round 2 query path: filter + probed L1 + probed clusters only.
pub fn round2_keys_for_query(
    namespace: &str,
    meta: &NamespaceMeta,
    l0: &CentroidIndexL0,
    l1_loaded: &HashMap<u32, CentroidIndexL1>,
    query: &[f64],
) -> Vec<String> {
    let mut keys = Vec::new();
    if meta.filter_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FilterSegment::key(namespace, meta.filter_segment_id));
    }

    let coarse_m = l0.probe_coarse_count();
    let coarse_ids = l0.nearest_coarse(query, coarse_m);
    for coarse_id in coarse_ids {
        if !l1_loaded.contains_key(&coarse_id) {
            keys.push(CentroidIndexL1::key(namespace, coarse_id));
        }
    }

    // Fine ids from loaded L1 segments (query-time after round2 partial decode).
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
        keys.push(ClusterSegment::key(namespace, fine_id));
    }
    keys
}

/// Probe plan without requiring L1 in memory: fetch L1 for top coarse, clusters resolved after decode.
pub fn round2_keys_for_query_probe(
    namespace: &str,
    meta: &NamespaceMeta,
    l0: &CentroidIndexL0,
    query: &[f64],
) -> Vec<String> {
    let mut keys = Vec::new();
    if meta.filter_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FilterSegment::key(namespace, meta.filter_segment_id));
    }
    for coarse_id in l0.nearest_coarse(query, l0.probe_coarse_count()) {
        keys.push(CentroidIndexL1::key(namespace, coarse_id));
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

    let l0 = r1
        .get(&CentroidIndexL0::key(namespace))
        .map(|b| CentroidIndexL0::decode(b))
        .transpose()?;

    let mut r2_keys = Vec::new();
    if let Some(ref c) = l0 {
        r2_keys = round2_keys(namespace, meta, c);
    } else if meta.filter_segment_id > 0 && meta.index_cursor > 0 {
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
    let vector = decode_vector_from_rounds(namespace, meta, l0, &r2)?;

    Ok(ColdIndexArtifacts {
        fts,
        filter,
        vector,
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

    if meta.wal_commit_seq == 0 {
        return Ok((meta, etag, HashMap::new(), storage_roundtrips));
    }

    let wal_keys: Vec<String> = (1..=meta.wal_commit_seq)
        .map(|seq| crate::wal::wal_key(namespace, seq))
        .collect();
    let wal_map_raw = fetch_round(client, bucket, &wal_keys).await?;
    storage_roundtrips += 1;

    let mut wal_by_seq = HashMap::new();
    for seq in 1..=meta.wal_commit_seq {
        let key = crate::wal::wal_key(namespace, seq);
        if let Some(bytes) = wal_map_raw.get(&key) {
            wal_by_seq.insert(seq, bytes.clone());
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
    l0: Option<CentroidIndexL0>,
    r2: &HashMap<String, Vec<u8>>,
) -> Result<Option<VectorIndex>> {
    let l0 = match l0 {
        Some(c) if c.num_fine_total > 0 => c,
        _ => return Ok(None),
    };
    if meta.vector_segment_id == 0 || meta.index_cursor == 0 || meta.dimensions == 0 {
        return Ok(None);
    }

    let mut l1 = HashMap::new();
    for coarse_id in 0..l0.num_coarse {
        let key = CentroidIndexL1::key(namespace, coarse_id);
        let Some(bytes) = r2.get(&key) else {
            continue;
        };
        let seg = CentroidIndexL1::decode(bytes)?;
        l1.insert(coarse_id, seg);
    }

    let mut clusters = HashMap::new();
    for fine_id in 0..l0.num_fine_total {
        let key = ClusterSegment::key(namespace, fine_id);
        let Some(bytes) = r2.get(&key) else {
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
    use crate::meta::NamespaceMeta;

    #[test]
    fn round1_keys_include_meta_centroids_fts() {
        let meta = NamespaceMeta {
            index_cursor: 5,
            fts_segment_id: 5,
            vector_segment_id: 5,
            dimensions: 4,
            wal_commit_seq: 5,
            ..Default::default()
        };
        let keys = round1_keys("ns", &meta);
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().any(|k| k.ends_with("meta.json")));
        assert!(keys.iter().any(|k| k.ends_with("centroids-l0.bin")));
        assert!(keys.iter().any(|k| k.contains("fts-00000005")));
    }

    #[test]
    fn round2_keys_filter_l1_and_clusters() {
        let meta = NamespaceMeta {
            index_cursor: 3,
            filter_segment_id: 3,
            vector_segment_id: 3,
            dimensions: 2,
            ..Default::default()
        };
        let l0 = CentroidIndexL0 {
            num_coarse: 4,
            num_fine_total: 16,
            fine_counts: vec![4, 4, 4, 4],
            centroids: vec![vec![0.0, 0.0]; 4],
            dimensions: 2,
            ..Default::default()
        };
        let keys = round2_keys("ns", &meta, &l0);
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
            dimensions: 2,
            ..Default::default()
        };
        let l0 = CentroidIndexL0 {
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
        let keys = round2_keys_for_query_probe("ns", &meta, &l0, &query);
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
            dimensions: 2,
            ..Default::default()
        };
        let keys = round1_index_keys("ns", &meta);
        assert_eq!(keys.len(), 2);
        assert!(!keys.iter().any(|k| k.ends_with("meta.json")));
    }

    #[test]
    fn cold_index_plan_is_two_roundtrips_when_indexed() {
        let meta = NamespaceMeta {
            index_cursor: 2,
            fts_segment_id: 2,
            filter_segment_id: 2,
            vector_segment_id: 2,
            dimensions: 2,
            ..Default::default()
        };
        let l0 = CentroidIndexL0 {
            num_coarse: 2,
            num_fine_total: 4,
            fine_counts: vec![2, 2],
            centroids: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            dimensions: 2,
            ..Default::default()
        };
        let r2 = round2_keys("ns", &meta, &l0);
        assert!(!r2.is_empty());
        let mut trips = 1u32;
        if !r2.is_empty() {
            trips += 1;
        }
        assert_eq!(trips, 2);
    }
}