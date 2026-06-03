//! Background indexer: merge WAL batches into FTS + vector indexes on S3 and advance `index_cursor`.

use crate::index::fts::FtsSegment;
use crate::index::vector::{primary_vector_field, CentroidIndex, ClusterSegment, VectorIndex};
use crate::meta::{meta_key, NamespaceMeta, META_RETRIES};
use crate::namespace::{fetch_meta, replay_wal_entries};

use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::collections::HashMap;
use std::time::Duration;

/// Merge WAL `(index_cursor+1)..=wal_commit_seq` into index segments and CAS-advance `index_cursor`.
pub async fn index_wal_range(client: &Client, bucket: &str, namespace: &str) -> Result<()> {
    for attempt in 0..META_RETRIES {
        let Some((meta, meta_etag)) = fetch_meta(client, bucket, namespace).await? else {
            return Ok(());
        };

        if meta.index_cursor >= meta.wal_commit_seq {
            return Ok(());
        }

        let from = meta.index_cursor.saturating_add(1);
        let to = meta.wal_commit_seq;
        let entries = replay_wal_entries(client, bucket, namespace, from, to).await?;

        let field = primary_fts_field(&meta);
        let mut segment = load_fts_segment(client, bucket, namespace, meta.fts_segment_id, &field)
            .await?
            .unwrap_or_else(|| FtsSegment {
                segment_id: to,
                field: field.clone(),
                ..Default::default()
            });

        let mut upserts: Vec<(String, crate::models::Document)> = Vec::new();
        let mut deletes: Vec<String> = Vec::new();
        for entry in &entries {
            deletes.extend(entry.deletes.clone());
            for doc in entry.clone().into_documents()? {
                upserts.push((doc.id.clone(), doc));
            }
        }
        segment.apply_delta(&upserts, &deletes);
        segment.segment_id = to;

        let key = FtsSegment::key(namespace, to);
        let body = segment.encode()?;
        client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from(body))
            .send()
            .await
            .with_context(|| format!("put fts segment {to:08}"))?;

        let mut vector_segment_id = meta.vector_segment_id;
        let mut vector_field = meta.vector_field.clone();
        let mut dimensions = meta.dimensions;

        if let Some(vfield) = primary_vector_field(&meta.schema, upserts.first().map(|(_, d)| d)) {
            let all_docs = docs_at_index_cursor(client, bucket, namespace, to).await?;
            let pairs: Vec<(String, crate::models::Document)> =
                all_docs.into_iter().collect();
            if let Some(vindex) = VectorIndex::build(
                to,
                &vfield,
                meta.distance_metric,
                &pairs,
            )? {
                write_vector_index(client, bucket, namespace, &vindex).await?;
                vector_segment_id = to;
                vector_field = vfield;
                dimensions = vindex.centroids.dimensions;
            }
        }

        let next_meta = meta_after_index_commit(
            &meta,
            to,
            to,
            vector_segment_id,
            vector_field,
            dimensions,
        )?;

        let meta_body = serde_json::to_vec(&next_meta)?;
        let mkey = meta_key(namespace);
        let mut put = client
            .put_object()
            .bucket(bucket)
            .key(&mkey)
            .body(ByteStream::from(meta_body));

        if let Some(etag) = &meta_etag {
            put = put.if_match(etag);
        } else {
            put = put.if_none_match("*");
        }

        match put.send().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                let service = e.into_service_error();
                let conflict = service.meta().code() == Some("PreconditionFailed");
                if conflict && attempt + 1 < META_RETRIES {
                    tokio::time::sleep(Duration::from_millis(50 * (attempt as u64 + 1))).await;
                    continue;
                }
                return Err(anyhow!("put meta after index: {service}"));
            }
        }
    }
    Err(anyhow!("meta CAS failed after index retries"))
}

async fn write_vector_index(
    client: &Client,
    bucket: &str,
    namespace: &str,
    vindex: &VectorIndex,
) -> Result<()> {
    let ckey = CentroidIndex::key(namespace);
    let cbody = vindex.centroids.encode()?;
    client
        .put_object()
        .bucket(bucket)
        .key(&ckey)
        .body(ByteStream::from(cbody))
        .send()
        .await
        .context("put centroids.bin")?;

    for (cid, cluster) in &vindex.clusters {
        let key = ClusterSegment::key(namespace, *cid);
        let body = cluster.encode()?;
        client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from(body))
            .send()
            .await
            .with_context(|| format!("put cluster {cid:08}"))?;
    }
    Ok(())
}

/// Run indexer after a durable WAL flush (v1: synchronous in write path).
pub async fn index_namespace(client: &Client, bucket: &str, namespace: &str) -> Result<()> {
    index_wal_range(client, bucket, namespace).await
}

fn primary_fts_field(meta: &NamespaceMeta) -> String {
    let fields = crate::index::fts::index_fields_from_schema(&meta.schema);
    fields.into_iter().next().unwrap_or_default()
}

async fn load_fts_segment(
    client: &Client,
    bucket: &str,
    namespace: &str,
    segment_id: u64,
    expected_field: &str,
) -> Result<Option<FtsSegment>> {
    if segment_id == 0 {
        return Ok(None);
    }
    let key = FtsSegment::key(namespace, segment_id);
    let out = client.get_object().bucket(bucket).key(&key).send().await;
    match out {
        Ok(resp) => {
            let bytes = resp
                .body
                .collect()
                .await
                .context("read fts segment")?
                .into_bytes();
            let seg = FtsSegment::decode(&bytes)?;
            if !expected_field.is_empty() && seg.field != expected_field {
                // Schema field changed; rebuild from WAL up to index_cursor would be ideal.
                // v1: keep loaded segment if non-empty, else empty.
            }
            Ok(Some(seg))
        }
        Err(e) => {
            let service = e.into_service_error();
            if service.is_no_such_key() {
                Ok(None)
            } else {
                Err(anyhow!("get fts segment: {service}"))
            }
        }
    }
}

/// CAS payload after indexer merges WAL through `index_cursor`.
pub fn meta_after_index_commit(
    meta: &NamespaceMeta,
    index_cursor: u64,
    fts_segment_id: u64,
    vector_segment_id: u64,
    vector_field: String,
    dimensions: u32,
) -> Result<NamespaceMeta> {
    if index_cursor > meta.wal_commit_seq {
        return Err(anyhow!(
            "index_cursor {index_cursor} past wal_commit_seq {}",
            meta.wal_commit_seq
        ));
    }
    if index_cursor <= meta.index_cursor {
        return Err(anyhow!(
            "index_cursor {index_cursor} does not advance from {}",
            meta.index_cursor
        ));
    }
    let mut next = meta.clone();
    next.index_cursor = index_cursor;
    next.fts_segment_id = fts_segment_id;
    next.vector_segment_id = vector_segment_id;
    next.vector_field = vector_field;
    next.dimensions = dimensions;
    Ok(next)
}

/// Load FTS segment for queries (returns None if not yet indexed).
pub async fn load_fts_segment_for_query(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
) -> Result<Option<FtsSegment>> {
    if meta.fts_segment_id == 0 || meta.index_cursor == 0 {
        return Ok(None);
    }
    load_fts_segment(client, bucket, namespace, meta.fts_segment_id, &primary_fts_field(meta))
        .await
}

/// Load vector ANN index (centroids + all cluster segments) for queries.
pub async fn load_vector_index_for_query(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
) -> Result<Option<VectorIndex>> {
    if meta.vector_segment_id == 0 || meta.index_cursor == 0 || meta.dimensions == 0 {
        return Ok(None);
    }
    let ckey = CentroidIndex::key(namespace);
    let out = client.get_object().bucket(bucket).key(&ckey).send().await;
    let centroids = match out {
        Ok(resp) => {
            let bytes = resp
                .body
                .collect()
                .await
                .context("read centroids.bin")?
                .into_bytes();
            CentroidIndex::decode(&bytes)?
        }
        Err(e) => {
            let service = e.into_service_error();
            if service.is_no_such_key() {
                return Ok(None);
            }
            return Err(anyhow!("get centroids: {service}"));
        }
    };

    let mut clusters = HashMap::new();
    for cid in 0..centroids.num_centroids {
        let key = ClusterSegment::key(namespace, cid);
        let seg_out = client.get_object().bucket(bucket).key(&key).send().await;
        match seg_out {
            Ok(resp) => {
                let bytes = resp
                    .body
                    .collect()
                    .await
                    .with_context(|| format!("read cluster {cid:08}"))?
                    .into_bytes();
                let seg = ClusterSegment::decode(&bytes)?;
                clusters.insert(cid, seg);
            }
            Err(e) => {
                let service = e.into_service_error();
                if !service.is_no_such_key() {
                    return Err(anyhow!("get cluster {cid}: {service}"));
                }
            }
        }
    }

    Ok(Some(VectorIndex {
        centroids,
        clusters,
    }))
}

/// Collect all documents up to `index_cursor` by replaying WAL (for vector rebuild / tests).
pub async fn docs_at_index_cursor(
    client: &Client,
    bucket: &str,
    namespace: &str,
    index_cursor: u64,
) -> Result<HashMap<String, crate::models::Document>> {
    let mut docs = HashMap::new();
    if index_cursor > 0 {
        crate::namespace::replay_wal_range(client, bucket, namespace, &mut docs, 1, index_cursor)
            .await?;
    }
    Ok(docs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::NamespaceMeta;

    #[test]
    fn meta_after_index_commit_advances_cursor() {
        let meta = NamespaceMeta {
            wal_commit_seq: 5,
            index_cursor: 2,
            ..Default::default()
        };
        let next = meta_after_index_commit(&meta, 5, 5, 5, "emb".into(), 3).unwrap();
        assert_eq!(next.index_cursor, 5);
        assert_eq!(next.fts_segment_id, 5);
        assert_eq!(next.vector_segment_id, 5);
        assert_eq!(next.vector_field, "emb");
        assert_eq!(next.dimensions, 3);
    }

    #[test]
    fn meta_after_index_commit_rejects_stale() {
        let meta = NamespaceMeta {
            index_cursor: 5,
            wal_commit_seq: 5,
            ..Default::default()
        };
        assert!(meta_after_index_commit(&meta, 5, 5, 0, String::new(), 0).is_err());
    }
}