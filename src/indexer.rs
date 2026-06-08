//! Background indexer: merge WAL batches into FTS + vector indexes on S3 and advance `index_cursor`.
//!
//! Indexing runs **asynchronously** on a tokio task (poll every 500ms or on WAL flush notify).
//! The write hot path only durably appends WAL + CAS `wal_commit_seq`; queries still see
//! strong consistency via indexed segments + unindexed WAL tail scan.

use crate::cache::SegmentCache;
use crate::config::AnnBuildConfig;
use crate::index::filter::FilterSegment;
use crate::index::fts::FtsSegment;
use crate::index::vector::{
    vector_fields_to_index, CentroidIndexL0, CentroidIndexL1, CentroidIndexL2, CentroidRouting,
    ClusterSegment, VectorIndex,
};
use crate::meta::{
    meta_key, push_segment_id, sync_legacy_vector_fields, vector_index_uses_legacy_paths,
    NamespaceMeta, VectorFieldConfig, META_RETRIES,
};
use crate::schema::vector_element_for_field;
use crate::namespace::{
    docs_at_index_cursor, fetch_meta, read_wal_entry, replay_wal_entries,
};
use crate::wal::{collect_index_delta, WalEntry};

use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

/// Merge WAL `(index_cursor+1)..=wal_commit_seq` into index segments and CAS-advance `index_cursor`.
///
/// When `max_segments` is `Some(n)`, indexes at most `n` WAL files per call (fair background slices).
pub async fn index_wal_range(
    client: &Client,
    bucket: &str,
    namespace: &str,
    cache: &Arc<SegmentCache>,
    ann_build: AnnBuildConfig,
    max_segments: Option<u64>,
) -> Result<()> {
    for attempt in 0..META_RETRIES {
        let Some((meta, _)) = fetch_meta(client, bucket, namespace).await? else {
            return Ok(());
        };

        if meta.index_cursor >= meta.wal_commit_seq {
            return Ok(());
        }

        let from = meta.index_cursor.saturating_add(1);
        let mut to = meta.wal_commit_seq;
        if let Some(max) = max_segments {
            to = to.min(from.saturating_add(max.saturating_sub(1)));
        }

        let field = primary_fts_field(&meta);
        let mut segment =
            load_fts_segment(client, bucket, namespace, meta.fts_segment_id, &field, cache)
                .await?
            .unwrap_or_else(|| FtsSegment {
                segment_id: to,
                field: field.clone(),
                ..Default::default()
            });

        let mut filter_segment = load_filter_segment(
            client,
            bucket,
            namespace,
            meta.filter_segment_id,
            cache,
        )
        .await?
        .unwrap_or_else(|| FilterSegment {
            segment_id: to,
            ..Default::default()
        });

        let (upserts, deletes) = stream_index_delta_from_wal(
            client,
            bucket,
            namespace,
            meta.index_cursor,
            from,
            to,
            &mut segment,
            &mut filter_segment,
            &meta.schema,
        )
        .await?;
        segment.segment_id = to;
        filter_segment.segment_id = to;
        let filter_key = FilterSegment::key(namespace, to);
        let filter_body = filter_segment.encode()?;
        let filter_resp = client
            .put_object()
            .bucket(bucket)
            .key(&filter_key)
            .body(ByteStream::from(filter_body.clone()))
            .send()
            .await
            .with_context(|| format!("put filter segment {to:08}"))?;
        cache.populate_after_put(
            bucket,
            &filter_key,
            &filter_body,
            filter_resp.e_tag(),
        );

        let key = FtsSegment::key(namespace, to);
        let body = segment.encode()?;
        let fts_resp = client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .with_context(|| format!("put fts segment {to:08}"))?;
        cache.populate_after_put(bucket, &key, &body, fts_resp.e_tag());

        let mut vector_fields = meta.vector_fields.clone();
        if vector_fields.is_empty() && !meta.vector_field.is_empty() && meta.dimensions > 0 {
            vector_fields.push(VectorFieldConfig {
                name: meta.vector_field.clone(),
                dimensions: meta.dimensions,
                element: vector_element_for_field(&meta.schema, &meta.vector_field),
                segment_id: meta.vector_segment_id,
                segment_ids: meta.vector_segment_ids.clone(),
            });
        }

        for vfield_name in
            vector_fields_to_index(&meta.schema, &meta, upserts.first().map(|(_, d)| d))
        {
            let mut vindex = load_vector_index_full_for_field(
                client,
                bucket,
                namespace,
                &meta,
                &vfield_name,
                cache,
                ann_build,
            )
            .await?
            .unwrap_or_default();

            if vindex.l0.num_fine_total == 0 {
                let pairs: Vec<(String, crate::models::Document)> = upserts.clone();
                if let Some(built) = VectorIndex::build(
                    to,
                    &vfield_name,
                    meta.distance_metric,
                    &pairs,
                    &meta.schema,
                    ann_build,
                )? {
                    vindex = built;
                }
            } else {
                vindex.apply_delta(&upserts, &deletes)?;
                if vindex.needs_full_rebuild() {
                    let all_docs = docs_at_index_cursor(client, bucket, namespace, to).await?;
                    let pairs: Vec<(String, crate::models::Document)> =
                        all_docs.into_iter().collect();
                    if let Some(built) = VectorIndex::build(
                        to,
                        &vfield_name,
                        meta.distance_metric,
                        &pairs,
                        &meta.schema,
                        ann_build,
                    )? {
                        vindex = built;
                    }
                }
            }

            if vindex.l0.num_fine_total > 0 {
                vindex.l0.segment_id = to;
                for l1 in vindex.l1.values_mut() {
                    l1.segment_id = to;
                }
                for cluster in vindex.clusters.values_mut() {
                    cluster.segment_id = to;
                }
                write_vector_index(client, bucket, namespace, &vfield_name, &vindex, cache)
                    .await?;
                let element = vector_element_for_field(&meta.schema, &vfield_name);
                let entry = vector_fields
                    .iter_mut()
                    .find(|f| f.name == vfield_name)
                    .map(|f| {
                        f.dimensions = vindex.l0.dimensions;
                        f.element = element;
                        f.segment_id = to;
                        push_segment_id(&mut f.segment_ids, to);
                        f.clone()
                    })
                    .unwrap_or_else(|| {
                        let mut ids = Vec::new();
                        push_segment_id(&mut ids, to);
                        VectorFieldConfig {
                            name: vfield_name.clone(),
                            dimensions: vindex.l0.dimensions,
                            element,
                            segment_id: to,
                            segment_ids: ids,
                        }
                    });
                if let Some(slot) = vector_fields.iter_mut().find(|f| f.name == vfield_name) {
                    *slot = entry;
                } else if vector_fields.len() < crate::meta::MAX_VECTOR_FIELDS {
                    vector_fields.push(entry);
                }
            }
        }

        let Some((fresh_meta, _)) = fetch_meta(client, bucket, namespace).await? else {
            return Ok(());
        };
        if fresh_meta.index_cursor >= fresh_meta.wal_commit_seq {
            return Ok(());
        }
        if fresh_meta.index_cursor >= to {
            return Ok(());
        }
        if to > fresh_meta.wal_commit_seq {
            continue;
        }

        let commit_lock = crate::commit_lock::namespace_commit_lock(namespace).await;
        let _commit = commit_lock.lock().await;

        let Some((fresh_meta, fresh_etag)) = fetch_meta(client, bucket, namespace).await? else {
            return Ok(());
        };
        if fresh_meta.index_cursor >= to {
            return Ok(());
        }
        if to > fresh_meta.wal_commit_seq {
            continue;
        }

        let next_meta = meta_after_index_commit(
            &fresh_meta,
            to,
            to,
            to,
            vector_fields,
            ann_build.ann_version,
        )?;

        let meta_body = serde_json::to_vec(&next_meta)?;
        let mkey = meta_key(namespace);
        let mut put = client
            .put_object()
            .bucket(bucket)
            .key(&mkey)
            .body(ByteStream::from(meta_body));

        if let Some(etag) = &fresh_etag {
            put = put.if_match(etag);
        } else {
            put = put.if_none_match("*");
        }

        match put.send().await {
            Ok(_) => {
                drop(_commit);
                if let Err(e) = crate::wal_compaction::maybe_compact_wal(
                    client,
                    bucket,
                    namespace,
                    cache,
                )
                .await
                {
                    tracing::warn!("wal compaction for {namespace}: {e:#}");
                }
                return Ok(());
            }
            Err(e) => {
                let service = e.into_service_error();
                let conflict = service.meta().code() == Some("PreconditionFailed");
                if conflict && attempt + 1 < META_RETRIES {
                    drop(_commit);
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
    field: &str,
    vindex: &VectorIndex,
    cache: &Arc<SegmentCache>,
) -> Result<()> {
    // Publish `centroids-l0.bin` last. L0 defines the ANN probe plan (which L1/L2/cluster keys
    // probed loads fetch). Cold/warm paths read L0 in round 1, then dependent segments. If L0
    // is visible before cluster objects for probed fine IDs exist, concurrent S3 readers decode
    // missing or stale segments → flaky 500s in multi-instance integration (fixed by L0-last).
    let l0_key = CentroidIndexL0::key(namespace, field);
    let l0_body = vindex.l0.encode()?;

    for l1 in vindex.l1.values() {
        let key = CentroidIndexL1::key(namespace, field, l1.coarse_id);
        let body = l1.encode()?;
        let resp = client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .with_context(|| format!("put centroids-l1-{:08}", l1.coarse_id))?;
        cache.populate_after_put(bucket, &key, &body, resp.e_tag());
    }

    for (fine_id, cluster) in &vindex.clusters {
        let key = ClusterSegment::key(namespace, field, *fine_id);
        let body = cluster.encode()?;
        let resp = client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .with_context(|| format!("put cluster {fine_id:08}"))?;
        cache.populate_after_put(bucket, &key, &body, resp.e_tag());
    }

    if let Some(ref routing) = vindex.routing {
        let key = CentroidRouting::key(namespace, field);
        let body = routing.encode()?;
        let resp = client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .context("put centroids-routing.bin")?;
        cache.populate_after_put(bucket, &key, &body, resp.e_tag());
    }

    for ((coarse_id, l2_id), l2) in &vindex.l2 {
        let key = CentroidIndexL2::key(namespace, field, *coarse_id, *l2_id);
        let body = l2.encode()?;
        let resp = client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .with_context(|| format!("put centroids-l2-{coarse_id:08}-{l2_id:08}"))?;
        cache.populate_after_put(bucket, &key, &body, resp.e_tag());
    }

    let l0_resp = client
        .put_object()
        .bucket(bucket)
        .key(&l0_key)
        .body(ByteStream::from(l0_body.clone()))
        .send()
        .await
        .context("put centroids-l0.bin")?;
    cache.populate_after_put(bucket, &l0_key, &l0_body, l0_resp.e_tag());
    Ok(())
}

/// Poll interval when no WAL flush notification is pending.
pub const INDEX_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Max wall time spent indexing a single namespace per background tick when multiple namespaces compete.
pub const MAX_INDEX_TIME_PER_NS_PER_TICK: Duration = Duration::from_secs(2);

/// When unindexed WAL lag is at or above this, the indexer may process multiple segments per tick.
pub const HIGH_INDEX_LAG_SEGMENTS: u64 = 4;

/// Lag at or above this: no per-tick time slice (large namespaces must finish vector builds).
pub const BURST_INDEX_LAG_SEGMENTS: u64 = 8;

/// Max WAL segments merged per tick for a sole namespace (or burst-lag namespace).
pub const MAX_SEGMENTS_BURST_PER_TICK: u64 = 8;

/// Priority key for background indexer scheduling: unindexed WAL segment count.
pub fn index_scheduling_lag(meta: &NamespaceMeta) -> u64 {
    unindexed_wal_segments(meta)
}

/// WAL segments to index in one background tick (fair when many namespaces compete).
pub fn segments_per_background_tick(lag: u64, competing_namespaces: usize) -> u64 {
    if lag == 0 {
        return 1;
    }
    if competing_namespaces <= 1 || lag >= BURST_INDEX_LAG_SEGMENTS {
        return lag.min(MAX_SEGMENTS_BURST_PER_TICK).max(1);
    }
    if lag >= HIGH_INDEX_LAG_SEGMENTS {
        return 2;
    }
    1
}

/// Whether a per-tick time slice applies (multi-tenant fairness for small backlogs only).
pub fn background_tick_time_limit(lag: u64, competing_namespaces: usize) -> Option<Duration> {
    if competing_namespaces <= 1 || lag >= BURST_INDEX_LAG_SEGMENTS {
        None
    } else {
        Some(MAX_INDEX_TIME_PER_NS_PER_TICK)
    }
}

/// Sort namespace names by descending index lag (largest backlog first).
pub fn sort_namespaces_by_index_lag(names: &mut [String], lag: &HashMap<String, u64>) {
    names.sort_by(|a, b| {
        lag.get(b)
            .copied()
            .unwrap_or(0)
            .cmp(&lag.get(a).copied().unwrap_or(0))
    });
}

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

/// Async background indexer: one global task, fair round-robin across namespaces.
pub struct BackgroundIndexer {
    client: Client,
    bucket: String,
    cache: Arc<SegmentCache>,
    ann_build: AnnBuildConfig,
    /// Round-robin queue; reprioritized by lag at the start of each tick.
    queue: Mutex<VecDeque<String>>,
    /// Names currently in `queue` (dedupe for `wake`).
    queued: Mutex<HashSet<String>>,
    notify: Notify,
}

impl BackgroundIndexer {
    pub fn spawn(
        client: Client,
        bucket: String,
        cache: Arc<SegmentCache>,
        ann_build: AnnBuildConfig,
    ) -> Arc<Self> {
        let this = Arc::new(Self {
            client,
            bucket,
            cache,
            ann_build,
            queue: Mutex::new(VecDeque::new()),
            queued: Mutex::new(HashSet::new()),
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
        let name = namespace.to_string();
        let mut queued = self.queued.lock().await;
        if queued.insert(name.clone()) {
            self.queue.lock().await.push_back(name);
            self.notify.notify_one();
        }
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
        self.refresh_work_queue().await?;
        let round_len = self.queue.lock().await.len();
        let mut lag_by_ns = HashMap::new();
        for name in self.queue.lock().await.iter() {
            if let Some((meta, _)) =
                fetch_meta(&self.client, &self.bucket, name).await?
            {
                lag_by_ns.insert(name.clone(), index_scheduling_lag(&meta));
            }
        }
        for _ in 0..round_len {
            let Some(namespace) = self.queue.lock().await.pop_front() else {
                break;
            };
            let lag = lag_by_ns.get(&namespace).copied().unwrap_or(0);
            let max_segments = segments_per_background_tick(lag, round_len);
            let time_limit = background_tick_time_limit(lag, round_len);
            let still_work = match self
                .index_namespace_timeboxed(&namespace, max_segments, time_limit)
                .await
            {
                Ok(needs_more) => needs_more,
                Err(e) => {
                    tracing::warn!(
                        "indexer failed for {namespace} (will retry): {e:#}"
                    );
                    true
                }
            };
            if still_work {
                self.queue.lock().await.push_back(namespace);
            } else {
                self.queued.lock().await.remove(&namespace);
            }
        }
        self.compaction_pass().await?;
        Ok(())
    }

    /// Run WAL compaction for every namespace that is fully indexed but still has stale segments.
    async fn compaction_pass(&self) -> Result<()> {
        for name in list_namespace_names(&self.client, &self.bucket).await? {
            let Some((meta, _)) = fetch_meta(&self.client, &self.bucket, &name).await?
            else {
                continue;
            };
            if crate::wal_compaction::needs_wal_compaction(&meta) {
                if let Err(e) = crate::wal_compaction::maybe_compact_wal(
                    &self.client,
                    &self.bucket,
                    &name,
                    &self.cache,
                )
                .await
                {
                    tracing::warn!("wal compaction for {name}: {e:#}");
                }
            }
        }
        Ok(())
    }

    /// Discover lagging namespaces, merge into the round-robin queue, sort by descending lag.
    async fn refresh_work_queue(&self) -> Result<()> {
        let mut names: HashSet<String> = self.queued.lock().await.clone();
        for name in list_namespace_names(&self.client, &self.bucket).await? {
            names.insert(name);
        }

        let mut lag = HashMap::new();
        let mut active = Vec::new();
        for name in names {
            let Some((meta, _)) = fetch_meta(&self.client, &self.bucket, &name).await? else {
                continue;
            };
            let l = index_scheduling_lag(&meta);
            let compaction = crate::wal_compaction::needs_wal_compaction(&meta);
            if l > 0 || compaction {
                lag.insert(name.clone(), l);
                active.push(name);
            }
        }

        sort_namespaces_by_index_lag(&mut active, &lag);

        let max_lag = lag.values().copied().max().unwrap_or(0);
        crate::metrics::set_index_lag_segments(max_lag);

        let mut queued = self.queued.lock().await;
        let mut queue = self.queue.lock().await;
        queue.clear();
        queued.clear();
        for name in active {
            queue.push_back(name.clone());
            queued.insert(name);
        }
        Ok(())
    }

    /// Index up to `max_segments` WAL files; optional per-tick time limit when multiple namespaces compete.
    async fn index_namespace_timeboxed(
        &self,
        namespace: &str,
        max_segments: u64,
        time_limit: Option<Duration>,
    ) -> Result<bool> {
        let work = index_wal_range(
            &self.client,
            &self.bucket,
            namespace,
            &self.cache,
            self.ann_build,
            Some(max_segments.max(1)),
        );
        let index_result = match time_limit {
            Some(limit) => tokio::time::timeout(limit, work).await,
            None => Ok(work.await),
        };

        match index_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                tracing::debug!(
                    "background indexer time slice ({time_limit:?}) exceeded for {namespace}"
                );
            }
        }

        let Some((meta, _)) = fetch_meta(&self.client, &self.bucket, namespace).await? else {
            return Ok(false);
        };
        let still_indexing = needs_indexing(&meta);
        if !still_indexing {
            if let Err(e) = crate::wal_compaction::maybe_compact_wal(
                &self.client,
                &self.bucket,
                namespace,
                &self.cache,
            )
            .await
            {
                tracing::warn!("wal compaction for {namespace}: {e:#}");
            }
        }

        Ok(still_indexing || crate::wal_compaction::needs_wal_compaction(&meta))
    }

    #[cfg(test)]
    async fn queued_namespaces(&self) -> VecDeque<String> {
        self.queue.lock().await.clone()
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

/// Default timeout for write `block_until_indexed: true`.
pub const BLOCK_UNTIL_INDEXED_TIMEOUT: Duration = Duration::from_secs(30);

/// Block until `index_cursor` catches up to `wal_commit_seq` (tests / integration).
pub async fn wait_until_indexed(
    client: &Client,
    bucket: &str,
    namespace: &str,
    timeout: Duration,
) -> Result<()> {
    wait_until_indexed_with_indexer(client, bucket, namespace, None, timeout).await
}

/// Like [`wait_until_indexed`], optionally nudging the background indexer each poll.
pub async fn wait_until_indexed_with_indexer(
    client: &Client,
    bucket: &str,
    namespace: &str,
    indexer: Option<&Arc<BackgroundIndexer>>,
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
        if let Some(idx) = indexer {
            idx.wake(namespace).await;
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
    cache: &Arc<SegmentCache>,
) -> Result<Option<FtsSegment>> {
    if segment_id == 0 {
        return Ok(None);
    }
    let key = FtsSegment::key(namespace, segment_id);
    let Some(bytes) = cache.get_bytes(client, bucket, &key).await? else {
        return Ok(None);
    };
    let seg = FtsSegment::decode(&bytes)?;
    if !expected_field.is_empty() && seg.field != expected_field {
        // Schema field changed; rebuild from WAL up to index_cursor would be ideal.
        // v1: keep loaded segment if non-empty, else empty.
    }
    Ok(Some(seg))
}

async fn load_filter_segment(
    client: &Client,
    bucket: &str,
    namespace: &str,
    segment_id: u64,
    cache: &Arc<SegmentCache>,
) -> Result<Option<FilterSegment>> {
    if segment_id == 0 {
        return Ok(None);
    }
    let key = FilterSegment::key(namespace, segment_id);
    let Some(bytes) = cache.get_bytes(client, bucket, &key).await? else {
        return Ok(None);
    };
    Ok(Some(FilterSegment::decode(&bytes)?))
}

/// CAS payload after indexer merges WAL through `index_cursor`.
pub fn meta_after_index_commit(
    meta: &NamespaceMeta,
    index_cursor: u64,
    fts_segment_id: u64,
    filter_segment_id: u64,
    vector_fields: Vec<VectorFieldConfig>,
    indexer_ann_version: u8,
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
    push_segment_id(&mut next.fts_segment_ids, fts_segment_id);
    next.filter_segment_id = filter_segment_id;
    push_segment_id(&mut next.filter_segment_ids, filter_segment_id);
    next.vector_fields = vector_fields;
    sync_legacy_vector_fields(&mut next);
    if indexer_ann_version >= crate::index::vector::ANN_VERSION_V3 {
        next.preferred_ann_version = crate::index::vector::ANN_VERSION_V3;
    }
    Ok(next)
}

/// Load filter index segment for queries.
pub async fn load_filter_segment_for_query(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
    cache: &Arc<SegmentCache>,
) -> Result<Option<FilterSegment>> {
    if meta.filter_segment_id == 0 || meta.index_cursor == 0 {
        return Ok(None);
    }
    load_filter_segment(client, bucket, namespace, meta.filter_segment_id, cache).await
}

/// Load FTS segment for queries (returns None if not yet indexed).
pub async fn load_fts_segment_for_query(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
    cache: &Arc<SegmentCache>,
) -> Result<Option<FtsSegment>> {
    if meta.fts_segment_id == 0 || meta.index_cursor == 0 {
        return Ok(None);
    }
    load_fts_segment(
        client,
        bucket,
        namespace,
        meta.fts_segment_id,
        &primary_fts_field(meta),
        cache,
    )
    .await
}

/// Load L0 centroids for all indexed vector fields (query bootstrap; L1/clusters are probed per query).
pub async fn load_vector_l0_for_query(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
    cache: &Arc<SegmentCache>,
    ann_build: AnnBuildConfig,
) -> Result<HashMap<String, CentroidIndexL0>> {
    let mut out = HashMap::new();
    for cfg in crate::meta::effective_vector_fields(meta) {
        if cfg.segment_id == 0 || meta.index_cursor == 0 || cfg.dimensions == 0 {
            continue;
        }
        if let Some(l0) = load_vector_l0_for_field(
            client,
            bucket,
            namespace,
            meta,
            &cfg.name,
            cache,
            ann_build,
        )
        .await?
        {
            if l0.num_fine_total > 0 {
                out.insert(cfg.name.clone(), l0);
            }
        }
    }
    Ok(out)
}

async fn load_vector_l0_for_field(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    cache: &Arc<SegmentCache>,
    ann_build: AnnBuildConfig,
) -> Result<Option<CentroidIndexL0>> {
    let cfg = crate::meta::effective_vector_fields(meta)
        .into_iter()
        .find(|f| f.name == field);
    let Some(cfg) = cfg else {
        return Ok(None);
    };
    if cfg.segment_id == 0 || meta.index_cursor == 0 || cfg.dimensions == 0 {
        return Ok(None);
    }

    let use_legacy = vector_index_uses_legacy_paths(meta, field);
    let l0_key = CentroidIndexL0::key(namespace, field);
    let l0_bytes = if let Some(b) = cache.get_bytes(client, bucket, &l0_key).await? {
        Some(b)
    } else if use_legacy {
        cache
            .get_bytes(client, bucket, &CentroidIndexL0::legacy_key(namespace))
            .await?
    } else {
        None
    };
    let Some(l0_bytes) = l0_bytes else {
        return Ok(None);
    };
    let l0 = CentroidIndexL0::decode(&l0_bytes)?;
    Ok(Some(
        l0.align_with_namespace_meta(meta, Some(ann_build))
            .clamp_probe_plan_for_query(),
    ))
}

/// Probed L1 + cluster load via segment cache (warm query path; mirrors [`crate::s3_batch::fetch_cold_vector_probed`]).
pub async fn load_vector_index_probed_for_query(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: CentroidIndexL0,
    query: &[f64],
    cache: &Arc<SegmentCache>,
) -> Result<(VectorIndex, u32)> {
    let l1_keys = crate::s3_batch::l1_keys_for_query_probe(namespace, meta, field, &l0, query)?;
    let mut fetched = fetch_index_objects_from_cache(client, bucket, cache, &l1_keys).await?;
    if let Some(routing_key) = crate::s3_batch::routing_key_for_field(namespace, field, &l0) {
        if let Some(bytes) = cache.get_bytes(client, bucket, &routing_key).await? {
            fetched.insert(routing_key, bytes);
        }
    }
    let routing = crate::s3_batch::decode_routing_probed(namespace, field, &l0, &fetched)?;
    let l2_keys = routing
        .as_ref()
        .map(|r| {
            crate::s3_batch::l2_keys_for_query_probe(namespace, field, &l0, r, query)
        })
        .unwrap_or_default();
    let l2_bytes = fetch_index_objects_from_cache(client, bucket, cache, &l2_keys).await?;
    fetched.extend(l2_bytes);
    let cluster_keys = crate::s3_batch::cluster_keys_for_query_after_l1(
        namespace, meta, field, &l0, &fetched, query,
    )?;
    let cluster_bytes =
        fetch_index_objects_from_cache(client, bucket, cache, &cluster_keys).await?;
    fetched.extend(cluster_bytes);
    crate::s3_batch::assemble_vector_index_probed(namespace, meta, field, l0, &fetched, query)
}

async fn fetch_index_objects_from_cache(
    client: &Client,
    bucket: &str,
    cache: &Arc<SegmentCache>,
    keys: &[String],
) -> Result<HashMap<String, Vec<u8>>> {
    let mut out = HashMap::new();
    for key in keys {
        if let Some(bytes) = cache.get_bytes(client, bucket, key).await? {
            out.insert(key.clone(), bytes);
        }
    }
    Ok(out)
}

/// Full vector index for one field (indexer maintenance, recall, warm prefetch — not per-query ANN).
pub async fn load_vector_index_full_for_field(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    cache: &Arc<SegmentCache>,
    ann_build: AnnBuildConfig,
) -> Result<Option<VectorIndex>> {
    let Some(l0) = load_vector_l0_for_field(
        client,
        bucket,
        namespace,
        meta,
        field,
        cache,
        ann_build,
    )
    .await?
    else {
        return Ok(None);
    };
    if l0.num_fine_total == 0 {
        return Ok(None);
    }

    let use_legacy = vector_index_uses_legacy_paths(meta, field);

    let mut l1 = HashMap::new();
    for coarse_id in 0..l0.num_coarse {
        let key = CentroidIndexL1::key(namespace, field, coarse_id);
        let bytes = if let Some(b) = cache.get_bytes(client, bucket, &key).await? {
            Some(b)
        } else if use_legacy {
            cache
                .get_bytes(
                    client,
                    bucket,
                    &CentroidIndexL1::legacy_key(namespace, coarse_id),
                )
                .await?
        } else {
            None
        };
        let Some(bytes) = bytes else {
            continue;
        };
        let seg = CentroidIndexL1::decode(&bytes)?;
        l1.insert(coarse_id, seg);
    }

    let mut clusters = HashMap::new();
    for fine_id in 0..l0.num_fine_total {
        let key = ClusterSegment::key(namespace, field, fine_id);
        let bytes = if let Some(b) = cache.get_bytes(client, bucket, &key).await? {
            Some(b)
        } else if use_legacy {
            cache
                .get_bytes(client, bucket, &ClusterSegment::legacy_key(namespace, fine_id))
                .await?
        } else {
            None
        };
        let Some(bytes) = bytes else {
            continue;
        };
        let seg = ClusterSegment::decode(&bytes)?;
        clusters.insert(fine_id, seg);
    }

    Ok(Some(VectorIndex {
        l0,
        l1,
        clusters,
        routing: None,
        l2: HashMap::new(),
    }))
}

/// Load full indexes for all vector fields (recall evaluation, explicit warm prefetch).
pub async fn load_vector_indexes_full_for_eval(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
    cache: &Arc<SegmentCache>,
    ann_build: AnnBuildConfig,
) -> Result<HashMap<String, VectorIndex>> {
    let mut out = HashMap::new();
    for cfg in crate::meta::effective_vector_fields(meta) {
        if let Some(idx) = load_vector_index_full_for_field(
            client,
            bucket,
            namespace,
            meta,
            &cfg.name,
            cache,
            ann_build,
        )
        .await?
        {
            out.insert(cfg.name.clone(), idx);
        }
    }
    Ok(out)
}

/// Replay WAL `from..=to` one segment at a time; apply FTS/filter deltas per batch.
async fn stream_index_delta_from_wal(
    client: &Client,
    bucket: &str,
    namespace: &str,
    index_cursor: u64,
    from: u64,
    to: u64,
    fts: &mut FtsSegment,
    filter: &mut FilterSegment,
    schema: &serde_json::Value,
) -> Result<(Vec<(String, crate::models::Document)>, Vec<String>)> {
    let fts_initial = fts.clone();
    let filter_initial = filter.clone();
    let mut upserts = Vec::new();
    let mut deletes = Vec::new();
    for seq in from..=to {
        let entry = read_wal_entry(client, bucket, namespace, seq).await?;
        if !entry.patches.is_empty() {
            *fts = fts_initial.clone();
            *filter = filter_initial.clone();
            let entries = replay_wal_entries(client, bucket, namespace, from, to).await?;
            let (u, d) =
                index_delta_from_wal_entries(client, bucket, namespace, index_cursor, &entries)
                    .await?;
            fts.apply_delta(&u, &d);
            filter.apply_delta(schema, &u, &d);
            return Ok((u, d));
        }
        let mut batch_deletes = entry.deletes.clone();
        let batch_upserts: Vec<(String, crate::models::Document)> = entry
            .into_documents()?
            .into_iter()
            .map(|doc| (doc.id.clone(), doc))
            .collect();
        fts.apply_delta(&batch_upserts, &batch_deletes);
        filter.apply_delta(schema, &batch_upserts, &batch_deletes);
        deletes.append(&mut batch_deletes);
        upserts.extend(batch_upserts);
    }
    Ok((upserts, deletes))
}

async fn index_delta_from_wal_entries(
    client: &Client,
    bucket: &str,
    namespace: &str,
    index_cursor: u64,
    entries: &[WalEntry],
) -> Result<(Vec<(String, crate::models::Document)>, Vec<String>)> {
    let has_patches = entries.iter().any(|e| !e.patches.is_empty());
    if !has_patches {
        let mut upserts = Vec::new();
        let mut deletes = Vec::new();
        for entry in entries {
            deletes.extend(entry.deletes.clone());
            for doc in entry.clone().into_documents()? {
                upserts.push((doc.id.clone(), doc));
            }
        }
        return Ok((upserts, deletes));
    }

    let mut baseline = docs_at_index_cursor(client, bucket, namespace, index_cursor).await?;
    collect_index_delta(&mut baseline, entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::vector::ANN_VERSION_V2;
    use crate::meta::NamespaceMeta;

    #[test]
    fn meta_after_index_commit_advances_cursor() {
        let meta = NamespaceMeta {
            wal_commit_seq: 5,
            index_cursor: 2,
            ..Default::default()
        };
        let vf = VectorFieldConfig {
            name: "emb".into(),
            dimensions: 3,
            segment_id: 5,
            segment_ids: vec![5],
            ..Default::default()
        };
        let next = meta_after_index_commit(&meta, 5, 5, 5, vec![vf], ANN_VERSION_V2).unwrap();
        assert_eq!(next.index_cursor, 5);
        assert_eq!(next.fts_segment_id, 5);
        assert_eq!(next.fts_segment_ids, vec![5]);
        assert_eq!(next.filter_segment_id, 5);
        assert_eq!(next.filter_segment_ids, vec![5]);
        assert_eq!(next.vector_segment_id, 5);
        assert_eq!(next.vector_segment_ids, vec![5]);
        assert_eq!(next.vector_field, "emb");
        assert_eq!(next.dimensions, 3);
        assert_eq!(next.vector_fields.len(), 1);
    }

    #[test]
    fn meta_after_index_commit_preserves_wal_commit_seq() {
        let meta = NamespaceMeta {
            wal_commit_seq: 10,
            index_cursor: 4,
            ..Default::default()
        };
        let next = meta_after_index_commit(&meta, 5, 5, 5, vec![], ANN_VERSION_V2).unwrap();
        assert_eq!(next.wal_commit_seq, 10);
        assert_eq!(next.index_cursor, 5);
    }

    #[test]
    fn meta_after_index_commit_rejects_stale() {
        let meta = NamespaceMeta {
            index_cursor: 5,
            wal_commit_seq: 5,
            ..Default::default()
        };
        assert!(meta_after_index_commit(&meta, 5, 5, 5, vec![], ANN_VERSION_V2).is_err());
    }

    #[test]
    fn meta_after_index_commit_sets_preferred_ann_version_v3() {
        use crate::index::vector::{ANN_VERSION_V2, ANN_VERSION_V3};
        let meta = NamespaceMeta {
            wal_commit_seq: 2,
            ..Default::default()
        };
        let next = meta_after_index_commit(&meta, 1, 1, 1, vec![], ANN_VERSION_V3).unwrap();
        assert_eq!(next.preferred_ann_version, ANN_VERSION_V3);
        let next_v2 = meta_after_index_commit(&next, 2, 2, 2, vec![], ANN_VERSION_V2).unwrap();
        assert_eq!(next_v2.preferred_ann_version, ANN_VERSION_V3);
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

    #[test]
    fn index_status_catching_up_and_up_to_date() {
        use crate::models::IndexStatus;
        assert_eq!(
            IndexStatus::from_meta(0, 3),
            IndexStatus::CatchingUp
        );
        assert_eq!(
            IndexStatus::from_meta(3, 3),
            IndexStatus::UpToDate
        );
        assert_eq!(
            IndexStatus::from_meta(5, 3),
            IndexStatus::UpToDate
        );
    }

    #[test]
    fn sort_namespaces_by_index_lag_orders_descending() {
        let mut names = vec![
            "low".to_string(),
            "hot".to_string(),
            "mid".to_string(),
        ];
        let lag = HashMap::from([
            ("low".to_string(), 1),
            ("hot".to_string(), 50),
            ("mid".to_string(), 10),
        ]);
        sort_namespaces_by_index_lag(&mut names, &lag);
        assert_eq!(names, vec!["hot", "mid", "low"]);
    }

    #[test]
    fn index_scheduling_lag_matches_unindexed_segments() {
        let meta = NamespaceMeta {
            index_cursor: 2,
            wal_commit_seq: 7,
            ..Default::default()
        };
        assert_eq!(index_scheduling_lag(&meta), 5);
    }

    #[test]
    fn segments_per_tick_burst_when_alone_or_large_lag() {
        assert_eq!(segments_per_background_tick(5, 3), 2);
        assert_eq!(segments_per_background_tick(3, 3), 1);
        assert_eq!(segments_per_background_tick(5, 1), 5);
        assert_eq!(segments_per_background_tick(20, 3), 8);
        assert_eq!(segments_per_background_tick(20, 1), 8);
    }

    #[test]
    fn background_time_limit_skipped_for_burst_lag() {
        assert!(background_tick_time_limit(8, 5).is_none());
        assert!(background_tick_time_limit(3, 1).is_none());
        assert_eq!(
            background_tick_time_limit(3, 3),
            Some(MAX_INDEX_TIME_PER_NS_PER_TICK)
        );
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
            cache: SegmentCache::disabled(),
            ann_build: AnnBuildConfig::default(),
            queue: Mutex::new(VecDeque::new()),
            queued: Mutex::new(HashSet::new()),
            notify: Notify::new(),
        };
        idx.wake("ns-a").await;
        idx.wake("ns-a").await;
        let queue = idx.queued_namespaces().await;
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.front().map(String::as_str), Some("ns-a"));
    }

    #[test]
    fn probed_cluster_ids_bounded_vs_full_index() {
        use crate::index::vector::{DEFAULT_PROBE_COARSE, DEFAULT_PROBE_FINE};
        use crate::index::vector::probe_fine_centroids_parts;

        let l0 = CentroidIndexL0 {
            vector_field: "emb".into(),
            num_coarse: 32,
            num_fine_total: 4000,
            fine_counts: vec![125; 32],
            centroids: vec![vec![0.0; 8]; 32],
            dimensions: 8,
            probe_coarse: DEFAULT_PROBE_COARSE,
            probe_fine: DEFAULT_PROBE_FINE,
            ..Default::default()
        };
        let query = vec![1.0; 8];
        let mut l1 = HashMap::new();
        for coarse_id in l0.nearest_coarse(&query, l0.probe_coarse_count()) {
            let start = l0.global_id_start(coarse_id);
            l1.insert(
                coarse_id,
                CentroidIndexL1 {
                    segment_id: 1,
                    coarse_id,
                    global_id_start: start,
                    num_fine: l0.fine_counts[coarse_id as usize],
                    centroids: (0..l0.fine_counts[coarse_id as usize])
                        .map(|i| vec![if i == 0 { 1.0 } else { 0.0 }; 8])
                        .collect(),
                },
            );
        }
        let probed = probe_fine_centroids_parts(&l0, &l1, None, &HashMap::new(), &query);
        assert!(probed.len() >= 8 && probed.len() <= 64);
        assert!(probed.len() < l0.num_fine_total as usize / 10);
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