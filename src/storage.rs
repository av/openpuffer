use crate::buffer::{WriteBufferConfig, WriteBufferManager};
use crate::models::Document;
use crate::view::NamespaceView;
use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::types::ObjectIdentifier;
use aws_sdk_s3::Client;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

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
    write_buffer: WriteBufferManager,
    views: Arc<RwLock<HashMap<String, NamespaceView>>>,
}

pub struct LoadedNamespace {
    pub docs: HashMap<String, Document>,
    pub meta_etag: Option<String>,
}

impl Storage {
    pub fn new(client: Client, bucket: String) -> Arc<Self> {
        let write_buffer =
            WriteBufferManager::new(client.clone(), bucket.clone(), WriteBufferConfig::default());
        Arc::new(Self {
            client,
            bucket,
            write_buffer,
            views: Arc::new(RwLock::new(HashMap::new())),
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
    pub async fn load_namespace(&self, name: &str) -> Result<LoadedNamespace> {
        let mut views = self.views.write().await;
        if let Some(view) = views.get_mut(name) {
            view.catch_up(&self.client, &self.bucket, name).await?;
            return Ok(LoadedNamespace {
                docs: view.docs.clone(),
                meta_etag: view.meta_etag.clone(),
            });
        }

        let mut view = NamespaceView::load(&self.client, &self.bucket, name).await?;
        view.catch_up(&self.client, &self.bucket, name).await?;
        let loaded = LoadedNamespace {
            docs: view.docs.clone(),
            meta_etag: view.meta_etag.clone(),
        };
        views.insert(name.to_string(), view);
        Ok(loaded)
    }

    pub fn invalidate_cache(&self, name: &str) {
        let name = name.to_string();
        let views = self.views.clone();
        tokio::spawn(async move {
            views.write().await.remove(&name);
        });
    }

    /// Durably append via group-commit buffer; ACK after WAL + meta CAS on S3.
    pub async fn write_documents(
        &self,
        namespace: &str,
        upserts: Vec<Document>,
        deletes: Vec<String>,
    ) -> Result<()> {
        let committed = self
            .write_buffer
            .write(namespace, upserts, deletes)
            .await?;

        let mut views = self.views.write().await;
        if let Some(view) = views.get_mut(namespace) {
            view.apply_committed(committed.seq, &committed.entry)?;
        } else {
            // Cold cache: load from S3 (flush already committed `seq` on object storage).
            let view = NamespaceView::load(&self.client, &self.bucket, namespace).await?;
            views.insert(namespace.to_string(), view);
        }
        Ok(())
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

        self.views.write().await.remove(name);
        Ok(())
    }
}

async fn namespace_exists(client: &Client, bucket: &str, name: &str) -> Result<bool> {
    if object_exists(client, bucket, &crate::meta::meta_key(name)).await? {
        return Ok(true);
    }
    if object_exists(client, bucket, &crate::models::manifest_key(name)).await? {
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