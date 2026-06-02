use crate::models::{self, Document, Manifest};
use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ObjectIdentifier;
use aws_sdk_s3::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

const MANIFEST_RETRIES: u32 = 8;

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
    manifest_etag: Option<String>,
}

pub struct LoadedNamespace {
    pub docs: HashMap<String, Document>,
    pub manifest_etag: Option<String>,
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
                .prefix(models::ROOT_PREFIX)
                .delimiter("/");
            if let Some(t) = &token {
                req = req.continuation_token(t);
            }
            let out = req.send().await.context("list namespaces")?;
            for cp in out.common_prefixes() {
                if let Some(p) = cp.prefix() {
                    let name = p
                        .strip_prefix(models::ROOT_PREFIX)
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
                        manifest_etag: entry.manifest_etag.clone(),
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
                manifest_etag: loaded.manifest_etag.clone(),
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
        let key = models::manifest_key(name);
        let (manifest, etag) = match self.get_manifest(&key).await? {
            Some((m, e)) => (m, e),
            None => {
                return Ok(LoadedNamespace {
                    docs: HashMap::new(),
                    manifest_etag: None,
                });
            }
        };

        let mut docs = HashMap::new();
        for id in &manifest.doc_ids {
            if let Some(doc) = self.get_document(name, id).await? {
                docs.insert(id.clone(), doc);
            }
        }
        Ok(LoadedNamespace {
            docs,
            manifest_etag: etag,
        })
    }

    async fn get_manifest(&self, key: &str) -> Result<Option<(Manifest, Option<String>)>> {
        let out = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;
        match out {
            Ok(resp) => {
                let etag = resp.e_tag().map(|s| s.to_string());
                let bytes = resp
                    .body
                    .collect()
                    .await
                    .context("read manifest body")?
                    .into_bytes();
                let manifest: Manifest =
                    serde_json::from_slice(&bytes).context("parse manifest")?;
                Ok(Some((manifest, etag)))
            }
            Err(e) => {
                let service = e.into_service_error();
                if service.is_no_such_key() {
                    Ok(None)
                } else if service.meta().code() == Some("NoSuchBucket") {
                    Err(anyhow!("S3 bucket not found"))
                } else {
                    Err(anyhow!("get manifest: {service}"))
                }
            }
        }
    }

    async fn get_document(&self, namespace: &str, id: &str) -> Result<Option<Document>> {
        let key = models::doc_key(namespace, id);
        let out = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await;
        match out {
            Ok(resp) => {
                let bytes = resp
                    .body
                    .collect()
                    .await
                    .context("read doc body")?
                    .into_bytes();
                let doc: Document = serde_json::from_slice(&bytes).context("parse document")?;
                Ok(Some(doc))
            }
            Err(e) => {
                let service = e.into_service_error();
                if service.is_no_such_key() {
                    Ok(None)
                } else {
                    Err(anyhow!("get document: {service}"))
                }
            }
        }
    }

    pub async fn write_documents(
        &self,
        namespace: &str,
        upserts: Vec<Document>,
        deletes: Vec<String>,
    ) -> Result<()> {
        for doc in &upserts {
            let body = serde_json::to_vec(doc)?;
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(models::doc_key(namespace, &doc.id))
                .body(ByteStream::from(body))
                .send()
                .await
                .context("put document")?;
        }
        for id in &deletes {
            let _ = self
                .client
                .delete_object()
                .bucket(&self.bucket)
                .key(models::doc_key(namespace, id))
                .send()
                .await;
        }

        for attempt in 0..MANIFEST_RETRIES {
            let key = models::manifest_key(namespace);
            let (mut doc_ids, manifest_etag) = match self.get_manifest(&key).await? {
                Some((m, e)) => (m.doc_ids, e),
                None => (Vec::new(), None),
            };
            for doc in &upserts {
                doc_ids.retain(|x| x != &doc.id);
                doc_ids.push(doc.id.clone());
            }
            for id in &deletes {
                doc_ids.retain(|x| x != id);
            }
            doc_ids.sort();
            doc_ids.dedup();

            let manifest = Manifest {
                doc_ids,
                schema_hints: serde_json::json!({}),
            };
            let body = serde_json::to_vec(&manifest)?;

            let mut put = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(ByteStream::from(body));

            if let Some(etag) = &manifest_etag {
                put = put.if_match(etag);
            } else {
                put = put.if_none_match("*");
            }

            match put.send().await {
                Ok(_) => {
                    self.cache.write().await.remove(namespace);
                    return Ok(());
                }
                Err(e) => {
                    let service = e.into_service_error();
                    let conflict = service.meta().code() == Some("PreconditionFailed");
                    if conflict && attempt + 1 < MANIFEST_RETRIES {
                        tokio::time::sleep(Duration::from_millis(50 * (attempt as u64 + 1))).await;
                        continue;
                    }
                    return Err(anyhow!("put manifest: {service}"));
                }
            }
        }
        Err(anyhow!("manifest write failed after retries"))
    }

    pub async fn delete_namespace(&self, name: &str) -> Result<()> {
        let manifest_key = models::manifest_key(name);
        let had_manifest = self.get_manifest(&manifest_key).await?.is_some();

        let prefix = models::namespace_prefix(name);
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

        if keys.is_empty() && !had_manifest {
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