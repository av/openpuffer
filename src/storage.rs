use crate::buffer::{WriteBufferConfig, WriteBufferManager};
use crate::config::{AnnProbeConfig, LimitsConfig};
use crate::limits::cap_filter_batch;
use crate::cache::SegmentCache;
use crate::filter::{parse_filter, should_apply_upsert};
use crate::indexer::{approx_unindexed_bytes, BackgroundIndexer};
use crate::models::IndexStatus;
use crate::index::fts::{wal_touched_doc_ids, FtsSegment};
use crate::index::filter::FilterSegment;
use crate::index::vector::VectorIndex;
use crate::meta::NamespaceMeta;
use crate::models::Document;
use crate::namespace::replay_wal_entries;
use crate::s3_batch::replay_wal_entries_batched;
use crate::search::{matching_doc_ids_for_filter, QueryConsistency, QueryContext};
use crate::export::{export_page, ExportPage, DEFAULT_EXPORT_LIMIT};
use crate::view::NamespaceView;
use crate::view_cache::ViewCache;
use crate::warm::{warm_namespace, WarmStats};
use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::types::ObjectIdentifier;
use aws_sdk_s3::Client;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Classify common S3 API failures for HTTP mapping.
pub fn s3_error_hint(err: &anyhow::Error) -> Option<&'static str> {
    let msg = format!("{err:#}");
    if msg.contains("NoSuchBucket") {
        return Some("bucket");
    }
    if msg.contains("XMinioInvalidObjectName") || msg.contains("InvalidObjectName") {
        return Some("invalid_object_name");
    }
    None
}

pub struct Storage {
    client: Client,
    bucket: String,
    cache: Arc<SegmentCache>,
    write_buffer: WriteBufferManager,
    views: Arc<Mutex<ViewCache>>,
    limits: LimitsConfig,
}

pub struct LoadedNamespace {
    pub docs: HashMap<String, Document>,
    pub meta: NamespaceMeta,
    pub meta_etag: Option<String>,
    pub fts: Option<FtsSegment>,
    pub vectors: HashMap<String, VectorIndex>,
    pub filter_index: Option<FilterSegment>,
    pub tail_doc_ids: HashSet<String>,
    /// Logical S3 roundtrips for cold load (index batch plan + optional WAL batch).
    pub storage_roundtrips: Option<u32>,
}

impl Storage {
    pub fn new(
        client: Client,
        bucket: String,
        cache: Arc<SegmentCache>,
        max_pinned_namespaces: usize,
        write_buffer_config: WriteBufferConfig,
        limits: LimitsConfig,
        ann_probes: AnnProbeConfig,
    ) -> Arc<Self> {
        let background_indexer = BackgroundIndexer::spawn(
            client.clone(),
            bucket.clone(),
            Arc::clone(&cache),
            ann_probes,
        );
        let write_buffer = WriteBufferManager::new(
            client.clone(),
            bucket.clone(),
            write_buffer_config,
            Some(background_indexer),
        );
        Arc::new(Self {
            client,
            bucket,
            cache,
            write_buffer,
            views: Arc::new(Mutex::new(ViewCache::new(max_pinned_namespaces))),
            limits,
        })
    }

    pub fn segment_cache(&self) -> &Arc<SegmentCache> {
        &self.cache
    }

    /// Shallow S3 probe for `GET /health?deep=1`.
    pub async fn deep_health_probe(&self) -> Result<()> {
        crate::health::probe_s3_storage(&self.client, &self.bucket).await
    }

    /// Namespace summary for list (no WAL replay for row count).
    pub async fn namespace_summary(&self, name: &str) -> Result<crate::models::NamespaceSummary> {
        let Some((meta, _)) =
            crate::namespace::fetch_meta(&self.client, &self.bucket, name).await?
        else {
            return Err(anyhow!("namespace not found"));
        };
        let unindexed_bytes =
            approx_unindexed_bytes(&self.client, &self.bucket, name, &meta).await;
        Ok(crate::models::NamespaceSummary {
            id: name.to_string(),
            index_cursor: Some(meta.index_cursor),
            wal_commit_seq: Some(meta.wal_commit_seq),
            unindexed_bytes: Some(unindexed_bytes),
            index_status: Some(IndexStatus::from_meta(meta.index_cursor, meta.wal_commit_seq)),
        })
    }

    /// Namespace metadata for turbopuffer-style observability.
    pub async fn namespace_metadata(&self, name: &str) -> Result<crate::models::NamespaceMetadata> {
        let Some((meta, _)) =
            crate::namespace::fetch_meta(&self.client, &self.bucket, name).await?
        else {
            return Err(anyhow!("namespace not found"));
        };
        let unindexed_bytes =
            approx_unindexed_bytes(&self.client, &self.bucket, name, &meta).await;
        let approx_row_count = self.approx_row_count(name, &meta).await?;
        Ok(crate::models::NamespaceMetadata {
            id: name.to_string(),
            index_cursor: meta.index_cursor,
            wal_commit_seq: meta.wal_commit_seq,
            approx_row_count,
            unindexed_bytes,
            index_status: IndexStatus::from_meta(meta.index_cursor, meta.wal_commit_seq),
        })
    }

    /// Document count: pinned view when caught up, else full WAL replay.
    async fn approx_row_count(&self, name: &str, meta: &NamespaceMeta) -> Result<u64> {
        {
            let mut views = self.views.lock().await;
            if let Some(view) = views.get_mut(name) {
                if view.last_applied_wal_seq >= meta.wal_commit_seq {
                    return Ok(view.docs.len() as u64);
                }
            }
        }
        let view = NamespaceView::load(&self.client, &self.bucket, name).await?;
        Ok(view.docs.len() as u64)
    }

    pub async fn list_namespaces(&self) -> Result<Vec<String>> {
        let mut namespaces = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(crate::models::ROOT_PREFIX)
                .delimiter("/");
            if let Some(t) = &token {
                req = req.continuation_token(t);
            }
            let out = req.send().await.context("list namespaces")?;
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

    /// Load namespace for query: cached [`NamespaceView`] with incremental WAL tail apply.
    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns `namespace not found` when the namespace has no `meta.json` or S3 prefix objects.
    pub async fn require_namespace(&self, name: &str) -> Result<()> {
        if namespace_exists(&self.client, &self.bucket, name).await? {
            Ok(())
        } else {
            Err(anyhow!("namespace not found"))
        }
    }

    /// Load namespace for query. `consistency` controls WAL tail work on the hot path.
    pub async fn load_namespace_for_query(
        &self,
        name: &str,
        consistency: QueryConsistency,
    ) -> Result<LoadedNamespace> {
        let cold_batch = !self.cache.enabled();
        let skip_wal_tail = consistency == QueryConsistency::Eventual;
        let mut views = self.views.lock().await;
        if let Some(view) = views.get_mut(name) {
            match consistency {
                QueryConsistency::Strong => {
                    view.catch_up(&self.client, &self.bucket, name).await?;
                }
                QueryConsistency::Eventual => {
                    // Refresh index segment pointers without WAL replay (sub-10ms warm path).
                    if let Ok(Some((meta, etag))) =
                        crate::namespace::fetch_meta(&self.client, &self.bucket, name).await
                    {
                        view.meta = meta;
                        view.meta_etag = etag;
                    }
                }
            }
            return self
                .loaded_from_view(name, view, cold_batch, 0, skip_wal_tail)
                .await;
        }

        let (mut view, wal_roundtrips) = if cold_batch {
            NamespaceView::load_cold_batched(&self.client, &self.bucket, name).await?
        } else {
            (NamespaceView::load(&self.client, &self.bucket, name).await?, 0)
        };
        if consistency == QueryConsistency::Strong {
            view.catch_up(&self.client, &self.bucket, name).await?;
        }
        let loaded = self
            .loaded_from_view(name, &view, cold_batch, wal_roundtrips, skip_wal_tail)
            .await?;
        views.insert(name.to_string(), view);
        Ok(loaded)
    }

    /// Strong-consistency load (filter writes, export helpers). Equivalent to `load_namespace_for_query(..., Strong)`.
    pub async fn load_namespace(&self, name: &str) -> Result<LoadedNamespace> {
        self.load_namespace_for_query(name, QueryConsistency::Strong)
            .await
    }

    /// Load namespace view only (WAL replay / pin) for export — no index segments.
    pub async fn load_view_snapshot(&self, name: &str) -> Result<NamespaceView> {
        if !namespace_exists(&self.client, &self.bucket, name).await? {
            return Err(anyhow!("namespace not found"));
        }

        let cold_batch = !self.cache.enabled();
        let mut views = self.views.lock().await;
        if let Some(view) = views.get_mut(name) {
            view.catch_up(&self.client, &self.bucket, name).await?;
            return Ok(view.clone());
        }

        let (mut view, _) = if cold_batch {
            NamespaceView::load_cold_batched(&self.client, &self.bucket, name).await?
        } else {
            (NamespaceView::load(&self.client, &self.bucket, name).await?, 0)
        };
        view.catch_up(&self.client, &self.bucket, name).await?;
        views.insert(name.to_string(), view.clone());
        Ok(view)
    }

    /// Export one page of documents at a consistent `wal_commit_seq` snapshot.
    pub async fn export_namespace_page(
        &self,
        name: &str,
        last_id: Option<&str>,
        limit: Option<usize>,
    ) -> Result<ExportPage> {
        let view = self.load_view_snapshot(name).await?;
        let limit = limit.unwrap_or(DEFAULT_EXPORT_LIMIT);
        Ok(export_page(
            &view.docs,
            view.meta.wal_commit_seq,
            last_id,
            limit,
        ))
    }

    /// Warm disk cache + pin in-memory view (turbopuffer `hint_cache_warm` analogue).
    pub async fn warm_namespace(&self, name: &str) -> Result<WarmStats> {
        let mut views = self.views.lock().await;
        warm_namespace(
            &self.client,
            &self.bucket,
            name,
            &self.cache,
            &mut views,
        )
        .await
    }

    async fn loaded_from_view(
        &self,
        name: &str,
        view: &NamespaceView,
        cold_batch: bool,
        wal_roundtrips: u32,
        skip_wal_tail: bool,
    ) -> Result<LoadedNamespace> {
        let (fts, vectors, filter_index, index_roundtrips) = if cold_batch {
            let art = crate::s3_batch::fetch_cold_index_artifacts(
                &self.client,
                &self.bucket,
                name,
                &view.meta,
            )
            .await?;
            (art.fts, art.vectors, art.filter, art.storage_roundtrips)
        } else {
            let fts = crate::indexer::load_fts_segment_for_query(
                &self.client,
                &self.bucket,
                name,
                &view.meta,
                &self.cache,
            )
            .await?;
            let vectors = crate::indexer::load_vector_indexes_for_query(
                &self.client,
                &self.bucket,
                name,
                &view.meta,
                &self.cache,
            )
            .await?;
            let filter_index = crate::indexer::load_filter_segment_for_query(
                &self.client,
                &self.bucket,
                name,
                &view.meta,
                &self.cache,
            )
            .await?;
            (fts, vectors, filter_index, 0)
        };

        let mut storage_roundtrips = if cold_batch {
            wal_roundtrips.saturating_add(index_roundtrips)
        } else {
            0
        };

        let tail_doc_ids = if skip_wal_tail {
            HashSet::new()
        } else if view.meta.index_cursor < view.meta.wal_commit_seq {
            let to = view.meta.wal_commit_seq;
            let from = if view.meta.wal_snapshot_seq > 0 {
                crate::wal_compaction::wal_replay_from(view.meta.wal_snapshot_seq, to)
                    .unwrap_or(view.meta.index_cursor.saturating_add(1))
            } else {
                view.meta.index_cursor.saturating_add(1)
            };
            if from > to {
                HashSet::new()
            } else {
                let entries = if cold_batch {
                    storage_roundtrips += 1;
                    replay_wal_entries_batched(&self.client, &self.bucket, name, from, to).await?
                } else {
                    replay_wal_entries(&self.client, &self.bucket, name, from, to).await?
                };
                wal_touched_doc_ids(&entries)
            }
        } else {
            HashSet::new()
        };

        let storage_roundtrips = if cold_batch {
            Some(storage_roundtrips)
        } else {
            None
        };
        Ok(LoadedNamespace {
            docs: view.docs.clone(),
            meta: view.meta.clone(),
            meta_etag: view.meta_etag.clone(),
            fts,
            vectors,
            filter_index,
            tail_doc_ids,
            storage_roundtrips,
        })
    }

    pub fn invalidate_cache(&self, name: &str) {
        self.cache.invalidate_namespace(&self.bucket, name);
        let name = name.to_string();
        let views = self.views.clone();
        tokio::spawn(async move {
            views.lock().await.remove(&name);
        });
    }

    /// Server-side S3 copy of all objects under `openpuffer/{source}/` → `openpuffer/{dest}/`.
    /// Destination must be empty; source must exist.
    pub async fn copy_from_namespace(&self, dest: &str, source: &str) -> Result<()> {
        self.clone_namespace_s3(dest, source).await
    }

    /// Instant COW branch: same S3 server-side copy as `copy_from_namespace`.
    pub async fn branch_from_namespace(&self, dest: &str, source: &str) -> Result<()> {
        self.clone_namespace_s3(dest, source).await
    }

    /// List + `copy_object` entire namespace prefix (shared by copy and branch).
    async fn clone_namespace_s3(&self, dest: &str, source: &str) -> Result<()> {
        if dest == source {
            return Err(anyhow!("cannot copy namespace to itself"));
        }
        if namespace_exists(&self.client, &self.bucket, dest).await? {
            return Err(anyhow!("destination namespace must be empty"));
        }
        let source_prefix = crate::models::namespace_prefix(source);
        if !namespace_exists(&self.client, &self.bucket, source).await? {
            return Err(anyhow!("source namespace not found"));
        }

        let dest_prefix = crate::models::namespace_prefix(dest);
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&source_prefix);
            if let Some(t) = &token {
                req = req.continuation_token(t);
            }
            let out = req.send().await.context("list source namespace objects")?;
            for obj in out.contents() {
                if let Some(k) = obj.key() {
                    keys.push(k.to_string());
                }
            }
            token = out.next_continuation_token().map(|s| s.to_string());
            if token.is_none() {
                break;
            }
        }

        if keys.is_empty() {
            return Err(anyhow!("source namespace not found"));
        }

        for key in &keys {
            let suffix = key
                .strip_prefix(&source_prefix)
                .ok_or_else(|| anyhow!("unexpected key prefix: {key}"))?;
            let dest_key = format!("{dest_prefix}{suffix}");
            let copy_source = format!("{}/{}", self.bucket, key);
            self.client
                .copy_object()
                .bucket(&self.bucket)
                .key(&dest_key)
                .copy_source(copy_source)
                .send()
                .await
                .with_context(|| format!("copy {key} -> {dest_key}"))?;
        }

        self.write_buffer.drop_namespace(dest).await;
        self.cache.invalidate_namespace(&self.bucket, dest);
        self.views.lock().await.remove(dest);
        Ok(())
    }

    /// Durably append via group-commit buffer; ACK after WAL + meta CAS on S3.
    pub async fn write_documents(
        &self,
        namespace: &str,
        upserts: Vec<Document>,
        mut patches: Vec<Document>,
        mut deletes: Vec<String>,
        schema_patch: Option<serde_json::Value>,
        delete_by_filter: Option<serde_json::Value>,
        delete_by_filter_allow_partial: bool,
        patch_by_filter: Option<(serde_json::Value, HashMap<String, serde_json::Value>)>,
        patch_by_filter_allow_partial: bool,
        upsert_condition: Option<serde_json::Value>,
        distance_metric: Option<crate::meta::DistanceMetric>,
        return_affected_ids: bool,
    ) -> Result<crate::models::WriteStats> {
        let max_filter = self.limits.max_filter_batch_rows;
        let mut rows_remaining = false;

        if let Some(filter_val) = delete_by_filter {
            if !filter_val.is_null() {
                let resolved = self
                    .resolve_ids_for_filter(namespace, &filter_val)
                    .await?;
                let (capped, remaining) =
                    cap_filter_batch(resolved, delete_by_filter_allow_partial, max_filter)
                        .map_err(anyhow::Error::msg)?;
                rows_remaining |= remaining;
                for id in capped {
                    if !deletes.contains(&id) {
                        deletes.push(id);
                    }
                }
            }
        }

        if let Some((filter_val, patch_attrs)) = patch_by_filter {
            let resolved = self
                .resolve_ids_for_filter(namespace, &filter_val)
                .await?;
            let (capped, remaining) =
                cap_filter_batch(resolved, patch_by_filter_allow_partial, max_filter)
                    .map_err(anyhow::Error::msg)?;
            rows_remaining |= remaining;
            for id in capped {
                if let Some(existing) = patches.iter_mut().find(|p| p.id == id) {
                    for (k, v) in &patch_attrs {
                        existing.attributes.insert(k.clone(), v.clone());
                    }
                } else {
                    patches.push(Document {
                        id,
                        attributes: patch_attrs.clone(),
                    });
                }
            }
        }

        let upserts = self
            .apply_upsert_condition(namespace, upserts, upsert_condition)
            .await?;

        let upserted_ids: Vec<String> = upserts.iter().map(|d| d.id.clone()).collect();
        let deleted_ids = deletes.clone();
        let stats = crate::models::WriteStats {
            rows_upserted: upserts.len() as u64,
            rows_patched: patches.len() as u64,
            rows_deleted: deletes.len() as u64,
            upserted_ids: return_affected_ids.then_some(upserted_ids),
            deleted_ids: return_affected_ids.then_some(deleted_ids),
            rows_remaining: rows_remaining.then_some(true),
        };

        let committed = self
            .write_buffer
            .write(
                namespace,
                upserts,
                patches,
                deletes,
                schema_patch,
                distance_metric,
                stats,
            )
            .await?;
        let stats = committed.stats;

        let mut views = self.views.lock().await;
        if let Some(view) = views.get_mut(namespace) {
            view.apply_committed(committed.seq, &committed.entry)?;
            if let Ok(Some((meta, etag))) =
                crate::namespace::fetch_meta(&self.client, &self.bucket, namespace).await
            {
                view.meta = meta;
                view.meta_etag = etag;
            }
        } else {
            // Cold cache: replay full WAL from S3 (includes the batch we just committed).
            // Do not `apply_committed` here — that would skip prior segments and duplicate
            // the latest batch if we loaded after append.
            let view = NamespaceView::load(&self.client, &self.bucket, namespace).await?;
            views.insert(namespace.to_string(), view);
        }
        Ok(stats)
    }

    /// Filter upserts by `upsert_condition` against committed namespace view (strong read).
    async fn apply_upsert_condition(
        &self,
        namespace: &str,
        upserts: Vec<Document>,
        upsert_condition: Option<serde_json::Value>,
    ) -> Result<Vec<Document>> {
        let Some(cond_val) = upsert_condition.filter(|v| !v.is_null()) else {
            return Ok(upserts);
        };
        let expr = parse_filter(&cond_val)?;
        let mut docs = self.load_docs_for_conditional_write(namespace).await?;
        self.write_buffer
            .overlay_pending_writes(namespace, &mut docs)
            .await?;
        let mut out = Vec::with_capacity(upserts.len());
        for doc in upserts {
            let current = docs.get(&doc.id);
            if should_apply_upsert(&expr, current, &doc) {
                out.push(doc);
            }
        }
        Ok(out)
    }

    /// Current doc map for conditional writes (empty if namespace not yet created).
    async fn load_docs_for_conditional_write(
        &self,
        namespace: &str,
    ) -> Result<HashMap<String, Document>> {
        if !namespace_exists(&self.client, &self.bucket, namespace).await? {
            return Ok(HashMap::new());
        }
        let view = self.load_view_snapshot(namespace).await?;
        Ok(view.docs)
    }

    /// Resolve doc ids for filter-based writes via filter index + WAL tail (strong consistency).
    async fn resolve_ids_for_filter(
        &self,
        namespace: &str,
        filter_val: &serde_json::Value,
    ) -> Result<Vec<String>> {
        let expr = parse_filter(filter_val)?;
        let loaded = self.load_namespace(namespace).await?;
        let ctx = QueryContext {
            docs: &loaded.docs,
            meta: &loaded.meta,
            fts: loaded.fts.as_ref(),
            vectors: &loaded.vectors,
            filter_index: loaded.filter_index.as_ref(),
            tail_doc_ids: &loaded.tail_doc_ids,
            consistency: QueryConsistency::Strong,
            storage_roundtrips: loaded.storage_roundtrips,
        };
        let ids = matching_doc_ids_for_filter(&ctx, &expr)?;
        Ok(ids.into_iter().collect())
    }

    /// Flush all pending write buffers (graceful shutdown).
    pub async fn flush_writes(&self) -> Result<()> {
        self.write_buffer.flush_all().await
    }

    pub async fn delete_namespace(&self, name: &str) -> Result<()> {
        let had_namespace = namespace_exists(&self.client, &self.bucket, name).await?;

        let prefix = crate::models::namespace_prefix(name);
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix);
            if let Some(t) = &token {
                req = req.continuation_token(t);
            }
            let out = req.send().await.context("list namespace objects")?;
            for obj in out.contents() {
                if let Some(k) = obj.key() {
                    keys.push(k.to_string());
                }
            }
            token = out.next_continuation_token().map(|s| s.to_string());
            if token.is_none() {
                break;
            }
        }

        if keys.is_empty() && !had_namespace {
            return Err(anyhow!("namespace not found"));
        }

        for chunk in keys.chunks(1000) {
            if chunk.is_empty() {
                continue;
            }
            let objects: Vec<ObjectIdentifier> = chunk
                .iter()
                .map(|k| {
                    ObjectIdentifier::builder()
                        .key(k)
                        .build()
                        .expect("valid key")
                })
                .collect();
            self.client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(
                    aws_sdk_s3::types::Delete::builder()
                        .set_objects(Some(objects))
                        .build()
                        .context("delete builder")?,
                )
                .send()
                .await
                .context("delete namespace objects")?;
        }

        self.views.lock().await.remove(name);
        Ok(())
    }
}

async fn namespace_exists(client: &Client, bucket: &str, name: &str) -> Result<bool> {
    if object_exists(client, bucket, &crate::meta::meta_key(name)).await? {
        return Ok(true);
    }
    let prefix = crate::models::namespace_prefix(name);
    let out = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(&prefix)
        .max_keys(1)
        .send()
        .await
        .context("list namespace for existence")?;
    Ok(out.key_count().unwrap_or(0) > 0)
}

async fn object_exists(client: &Client, bucket: &str, key: &str) -> Result<bool> {
    let out = client.head_object().bucket(bucket).key(key).send().await;
    match out {
        Ok(_) => Ok(true),
        Err(e) => {
            let service = e.into_service_error();
            if service.is_not_found() {
                Ok(false)
            } else {
                Err(anyhow!("head object: {service}"))
            }
        }
    }
}