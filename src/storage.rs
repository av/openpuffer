use crate::models::Document;
use crate::namespace::{self, LoadedNamespaceData};
use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::types::ObjectIdentifier;
use aws_sdk_s3::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
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
    cache: Arc<RwLock<HashMap<String, CachedNamespace>>>,
    cache_ttl: Duration,
}

struct CachedNamespace {
    loaded_at: Instant,
    docs: HashMap<String, Document>,
    meta_etag: Option<String>,
}

pub struct LoadedNamespace {
    pub docs: HashMap<String, Document>,
    pub meta_etag: Option<String>,
}

impl Storage {
    pub fn new(client: Client, bucket: String) -> Arc<Self> {
        Arc::new(Self {
            client,
            bucket,
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl: Duration::from_secs(30),
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

    pub async fn load_namespace(&self, name: &str) -> Result<LoadedNamespace> {
        {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(name) {
                if entry.loaded_at.elapsed() < self.cache_ttl {
                    return Ok(LoadedNamespace {
                        docs: entry.docs.clone(),
                        meta_etag: entry.meta_etag.clone(),
                    });
                }
            }
        }

        let loaded = self.load_namespace_from_s3(name).await?;
        let mut cache = self.cache.write().await;
        cache.insert(
            name.to_string(),
            CachedNamespace {
                loaded_at: Instant::now(),
                docs: loaded.docs.clone(),
                meta_etag: loaded.meta_etag.clone(),
            },
        );
        Ok(loaded)
    }

    pub fn invalidate_cache(&self, name: &str) {
        let name = name.to_string();
        let cache = self.cache.clone();
        tokio::spawn(async move {
            cache.write().await.remove(&name);
        });
    }

    async fn load_namespace_from_s3(&self, name: &str) -> Result<LoadedNamespace> {
        let LoadedNamespaceData { docs, meta_etag } =
            namespace::load_namespace(&self.client, &self.bucket, name).await?;
        Ok(LoadedNamespace { docs, meta_etag })
    }

    /// Durably append upserts/deletes via WAL + meta CAS (no per-doc JSON writes).
    pub async fn write_documents(
        &self,
        namespace: &str,
        upserts: Vec<Document>,
        deletes: Vec<String>,
    ) -> Result<()> {
        namespace::append_wal(
            &self.client,
            &self.bucket,
            namespace,
            upserts,
            deletes,
        )
        .await?;
        self.cache.write().await.remove(namespace);
        Ok(())
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

        self.cache.write().await.remove(name);
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