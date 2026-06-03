use crate::buffer::{WriteBufferConfig, WriteBufferManager};
use crate::cache::SegmentCache;
use crate::filter::parse_filter;
use crate::indexer::{approx_unindexed_bytes, BackgroundIndexer};
use crate::index::fts::{wal_touched_doc_ids, FtsSegment};
use crate::index::filter::FilterSegment;
use crate::index::vector::VectorIndex;
use crate::meta::NamespaceMeta;
use crate::models::Document;
use crate::namespace::replay_wal_entries;
use crate::search::{matching_doc_ids_for_filter, QueryConsistency, QueryContext};
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
}

pub struct LoadedNamespace {
    pub docs: HashMap<String, Document>,
    pub meta: NamespaceMeta,
    pub meta_etag: Option<String>,
    pub fts: Option<FtsSegment>,
    pub vector: Option<VectorIndex>,
    pub filter_index: Option<FilterSegment>,
    pub tail_doc_ids: HashSet<String>,
}

impl Storage {
    pub fn new(
        client: Client,
        bucket: String,
        cache: Arc<SegmentCache>,
        max_pinned_namespaces: usize,
    ) -> Arc<Self> {
        let background_indexer =
            BackgroundIndexer::spawn(client.clone(), bucket.clone(), Arc::clone(&cache));
        let write_buffer = WriteBufferManager::new(
            client.clone(),
            bucket.clone(),
            WriteBufferConfig::default(),
            Some(background_indexer),
        );
        Arc::new(Self {
            client,
            bucket,
            cache,
            write_buffer,
            views: Arc::new(Mutex::new(ViewCache::new(max_pinned_namespaces))),
        })
    }

    pub fn segment_cache(&self) -> &Arc<SegmentCache> {
        &self.cache
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
        Ok(crate::models::NamespaceMetadata {
            id: name.to_string(),
            index_cursor: meta.index_cursor,
            wal_commit_seq: meta.wal_commit_seq,
            unindexed_bytes,
        })
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

    pub async fn load_namespace(&self, name: &str) -> Result<LoadedNamespace> {
        let mut views = self.views.lock().await;
        if let Some(view) = views.get_mut(name) {
            view.catch_up(&self.client, &self.bucket, name).await?;
            return self.loaded_from_view(name, view).await;
        }

        let mut view = NamespaceView::load(&self.client, &self.bucket, name).await?;
        view.catch_up(&self.client, &self.bucket, name).await?;
        let loaded = self.loaded_from_view(name, &view).await?;
        views.insert(name.to_string(), view);
        Ok(loaded)
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

    async fn loaded_from_view(&self, name: &str, view: &NamespaceView) -> Result<LoadedNamespace> {
        let fts = crate::indexer::load_fts_segment_for_query(
            &self.client,
            &self.bucket,
            name,
            &view.meta,
            &self.cache,
        )
        .await?;
        let vector = crate::indexer::load_vector_index_for_query(
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
        let tail_doc_ids = if view.meta.index_cursor < view.meta.wal_commit_seq {
            let from = view.meta.index_cursor.saturating_add(1);
            let entries = replay_wal_entries(
                &self.client,
                &self.bucket,
                name,
                from,
                view.meta.wal_commit_seq,
            )
            .await?;
            wal_touched_doc_ids(&entries)
        } else {
            HashSet::new()
        };
        Ok(LoadedNamespace {
            docs: view.docs.clone(),
            meta: view.meta.clone(),
            meta_etag: view.meta_etag.clone(),
            fts,
            vector,
            filter_index,
            tail_doc_ids,
        })
    }

    pub fn invalidate_cache(&self, name: &str) {
        let name = name.to_string();
        let views = self.views.clone();
        tokio::spawn(async move {
            views.lock().await.remove(&name);
        });
    }

    /// Durably append via group-commit buffer; ACK after WAL + meta CAS on S3.
    pub async fn write_documents(
        &self,
        namespace: &str,
        upserts: Vec<Document>,
        mut deletes: Vec<String>,
        schema_patch: Option<serde_json::Value>,
        delete_by_filter: Option<serde_json::Value>,
    ) -> Result<()> {
        if let Some(filter_val) = delete_by_filter {
            if !filter_val.is_null() {
                let resolved = self
                    .resolve_delete_by_filter(namespace, &filter_val)
                    .await?;
                for id in resolved {
                    if !deletes.contains(&id) {
                        deletes.push(id);
                    }
                }
            }
        }

        let committed = self
            .write_buffer
            .write(namespace, upserts, deletes, schema_patch)
            .await?;

        let mut views = self.views.lock().await;
        if let Some(view) = views.get_mut(namespace) {
            view.apply_committed(committed.seq, &committed.entry)?;
            if let Some((meta, _)) =
                crate::namespace::fetch_meta(&self.client, &self.bucket, namespace).await?
            {
                view.meta = meta;
            }
        } else {
            let view = NamespaceView::load(&self.client, &self.bucket, namespace).await?;
            views.insert(namespace.to_string(), view);
        }
        Ok(())
    }

    /// Resolve doc ids for `delete_by_filter` via filter index + WAL tail (strong consistency).
    async fn resolve_delete_by_filter(
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
            vector: loaded.vector.as_ref(),
            filter_index: loaded.filter_index.as_ref(),
            tail_doc_ids: &loaded.tail_doc_ids,
            consistency: QueryConsistency::Strong,
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