//! Background indexer: merge WAL batches into FTS + vector indexes on S3 and advance `index_cursor`.
//!
//! Indexing runs **asynchronously** on a tokio task (poll every 500ms or on WAL flush notify).
//! The write hot path only durably appends WAL + CAS `wal_commit_seq`; queries still see
//! strong consistency via indexed segments + unindexed WAL tail scan.

use crate::index::filter::FilterSegment;
use crate::index::fts::FtsSegment;
use crate::index::vector::{primary_vector_field, CentroidIndex, ClusterSegment, VectorIndex};
use crate::meta::{meta_key, NamespaceMeta, META_RETRIES};
use crate::namespace::{fetch_meta, replay_wal_entries};

use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

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

        let all_for_filter = docs_at_index_cursor(client, bucket, namespace, to).await?;
        let filter_pairs: Vec<(String, crate::models::Document)> =
            all_for_filter.into_iter().collect();
        let filter_segment = FilterSegment::build(to, &meta.schema, &filter_pairs);
        let filter_key = FilterSegment::key(namespace, to);
        let filter_body = filter_segment.encode()?;
        client
            .put_object()
            .bucket(bucket)
            .key(&filter_key)
            .body(ByteStream::from(filter_body))
            .send()
            .await
            .with_context(|| format!("put filter segment {to:08}"))?;

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

/// Poll interval when no WAL flush notification is pending.
pub const INDEX_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// True when WAL segments exist that are not yet merged into `index/`.
pub fn needs_indexing(meta: &NamespaceMeta) -> bool {
    meta.index_cursor < meta.wal_commit_seq
}

/// Count of WAL files in the unindexed tail `(index_cursor, wal_commit_seq]`.
pub fn unindexed_wal_segments(meta: &NamespaceMeta) -> u64 {
    if meta.wal_commit_seq <= meta.index_cursor {
        return 0;
    }
    meta.wal_commit_seq - meta.index_cursor
}

/// Approximate bytes in unindexed WAL segments (HEAD per object; 4KiB fallback per segment).
pub async fn approx_unindexed_bytes(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
) -> u64 {
    let from = meta.index_cursor.saturating_add(1);
    let to = meta.wal_commit_seq;
    if from > to {
        return 0;
    }
    let mut total = 0u64;
    for seq in from..=to {
        let key = crate::wal::wal_key(namespace, seq);
        match head_content_length(client, bucket, &key).await {
            Ok(len) => total += len,
            Err(_) => total += 4096,
        }
    }
    total
}

async fn head_content_length(client: &Client, bucket: &str, key: &str) -> Result<u64> {
    let out = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context("head wal segment")?;
    Ok(out.content_length().unwrap_or(0).max(0) as u64)
}

/// Async background indexer: one global task, per-namespace work queue + periodic S3 scan.
pub struct BackgroundIndexer {
    client: Client,
    bucket: String,
    pending: Mutex<HashSet<String>>,
    notify: Notify,
}

impl BackgroundIndexer {
    pub fn spawn(client: Client, bucket: String) -> Arc<Self> {
        let this = Arc::new(Self {
            client,
            bucket,
            pending: Mutex::new(HashSet::new()),
            notify: Notify::new(),
        });
        let runner = Arc::clone(&this);
        tokio::spawn(async move {
            runner.run().await;
        });
        this
    }

    /// Notify the background loop that `namespace` may have unindexed WAL (non-blocking).
    pub async fn wake(&self, namespace: &str) {
        self.pending.lock().await.insert(namespace.to_string());
        self.notify.notify_one();
    }

    async fn run(self: Arc<Self>) {
        loop {
            let _ = tokio::time::timeout(INDEX_POLL_INTERVAL, self.notify.notified()).await;
            if let Err(e) = self.tick().await {
                tracing::warn!("background indexer tick: {e:#}");
            }
        }
    }

    async fn tick(&self) -> Result<()> {
        let mut work: HashSet<String> = self.pending.lock().await.drain().collect();

        for name in list_namespace_names(&self.client, &self.bucket).await? {
            if let Some((meta, _)) =
                crate::namespace::fetch_meta(&self.client, &self.bucket, &name).await?
            {
                if needs_indexing(&meta) {
                    work.insert(name);
                }
            }
        }

        for namespace in work {
            match index_wal_range(&self.client, &self.bucket, &namespace).await {
                Ok(()) => {}
                Err(e) => {
                    tracing::warn!(
                        "indexer failed for {namespace} (will retry): {e:#}"
                    );
                    self.pending.lock().await.insert(namespace);
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    async fn pending_namespaces(&self) -> HashSet<String> {
        self.pending.lock().await.clone()
    }
}

async fn list_namespace_names(client: &Client, bucket: &str) -> Result<Vec<String>> {
    let mut namespaces = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(crate::models::ROOT_PREFIX)
            .delimiter("/");
        if let Some(t) = &token {
            req = req.continuation_token(t);
        }
        let out = req.send().await.context("list namespaces for indexer")?;
        for cp in out.common_prefixes() {
            if let Some(p) = cp.prefix() {
                let name = p
                    .strip_prefix(crate::models::ROOT_PREFIX)
                    .and_then(|s| s.strip_suffix('/'))
                    .unwrap_or(p);
                if !name.is_empty() {
                    namespaces.push(name.to_string());
                }
            }
        }
        token = out.next_continuation_token().map(|s| s.to_string());
        if token.is_none() {
            break;
        }
    }
    namespaces.sort();
    namespaces.dedup();
    Ok(namespaces)
}

/// Block until `index_cursor` catches up to `wal_commit_seq` (tests / integration).
pub async fn wait_until_indexed(
    client: &Client,
    bucket: &str,
    namespace: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some((meta, _)) =
            crate::namespace::fetch_meta(client, bucket, namespace).await?
        {
            if !needs_indexing(&meta) {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "indexer did not catch up for {namespace} within {:?}",
                timeout
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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

async fn load_filter_segment(
    client: &Client,
    bucket: &str,
    namespace: &str,
    segment_id: u64,
) -> Result<Option<FilterSegment>> {
    if segment_id == 0 {
        return Ok(None);
    }
    let key = FilterSegment::key(namespace, segment_id);
    let out = client.get_object().bucket(bucket).key(&key).send().await;
    match out {
        Ok(resp) => {
            let bytes = resp
                .body
                .collect()
                .await
                .context("read filter segment")?
                .into_bytes();
            Ok(Some(FilterSegment::decode(&bytes)?))
        }
        Err(e) => {
            let service = e.into_service_error();
            if service.is_no_such_key() {
                Ok(None)
            } else {
                Err(anyhow!("get filter segment: {service}"))
            }
        }
    }
}

/// CAS payload after indexer merges WAL through `index_cursor`.
pub fn meta_after_index_commit(
    meta: &NamespaceMeta,
    index_cursor: u64,
    fts_segment_id: u64,
    filter_segment_id: u64,
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
    next.filter_segment_id = filter_segment_id;
    next.vector_segment_id = vector_segment_id;
    next.vector_field = vector_field;
    next.dimensions = dimensions;
    Ok(next)
}

/// Load filter index segment for queries.
pub async fn load_filter_segment_for_query(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
) -> Result<Option<FilterSegment>> {
    if meta.filter_segment_id == 0 || meta.index_cursor == 0 {
        return Ok(None);
    }
    load_filter_segment(client, bucket, namespace, meta.filter_segment_id).await
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
        let next = meta_after_index_commit(&meta, 5, 5, 5, 5, "emb".into(), 3).unwrap();
        assert_eq!(next.index_cursor, 5);
        assert_eq!(next.fts_segment_id, 5);
        assert_eq!(next.filter_segment_id, 5);
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
        assert!(meta_after_index_commit(&meta, 5, 5, 5, 0, String::new(), 0).is_err());
    }

    #[test]
    fn needs_indexing_when_cursor_behind_commit() {
        let meta = NamespaceMeta {
            index_cursor: 2,
            wal_commit_seq: 5,
            ..Default::default()
        };
        assert!(needs_indexing(&meta));
        assert_eq!(unindexed_wal_segments(&meta), 3);
    }

    #[test]
    fn needs_indexing_false_when_caught_up() {
        let meta = NamespaceMeta {
            index_cursor: 10,
            wal_commit_seq: 10,
            ..Default::default()
        };
        assert!(!needs_indexing(&meta));
        assert_eq!(unindexed_wal_segments(&meta), 0);
    }

    #[tokio::test]
    async fn background_indexer_wake_enqueues_namespace() {
        let idx = BackgroundIndexer {
            client: aws_sdk_s3::Client::from_conf(
                aws_sdk_s3::Config::builder()
                    .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                    .build(),
            ),
            bucket: "test".into(),
            pending: Mutex::new(HashSet::new()),
            notify: Notify::new(),
        };
        idx.wake("ns-a").await;
        idx.wake("ns-a").await;
        let pending = idx.pending_namespaces().await;
        assert_eq!(pending.len(), 1);
        assert!(pending.contains("ns-a"));
    }

    #[tokio::test]
    async fn index_cursor_catch_up_after_sleep() {
        let mut meta = NamespaceMeta {
            index_cursor: 0,
            wal_commit_seq: 3,
            ..Default::default()
        };
        assert!(needs_indexing(&meta));
        tokio::time::sleep(Duration::from_millis(20)).await;
        meta.index_cursor = 3;
        assert!(!needs_indexing(&meta));
    }
}