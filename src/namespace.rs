//! Per-namespace WAL append and replay on S3.

use crate::meta::{meta_after_wal_commit, meta_key, next_wal_seq, NamespaceMeta, META_RETRIES};
use crate::models::{self, Document, Manifest};
use crate::wal::{apply_entry, decode, encode, wal_key, WalEntry};
use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::collections::HashMap;
use std::time::Duration;

pub struct LoadedNamespaceData {
    pub docs: HashMap<String, Document>,
    pub meta_etag: Option<String>,
}

/// Append one WAL batch and CAS-advance `meta.json`.
pub async fn append_wal(
    client: &Client,
    bucket: &str,
    namespace: &str,
    upserts: Vec<Document>,
    deletes: Vec<String>,
) -> Result<()> {
    let entry = WalEntry::from_write(upserts, deletes)?;

    for attempt in 0..META_RETRIES {
        let key = meta_key(namespace);
        let (meta, meta_etag) = match get_meta(client, bucket, &key).await? {
            Some((m, e)) => (m, e),
            None => (NamespaceMeta::default(), None),
        };

        let seq = next_wal_seq(&meta);
        let wal_body = encode(&entry)?;
        client
            .put_object()
            .bucket(bucket)
            .key(wal_key(namespace, seq))
            .body(ByteStream::from(wal_body))
            .send()
            .await
            .with_context(|| format!("put wal segment {seq:08}"))?;

        let next_meta = meta_after_wal_commit(&meta, seq)?;
        let body = serde_json::to_vec(&next_meta)?;

        let mut put = client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from(body));

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
                return Err(anyhow!("put meta after wal: {service}"));
            }
        }
    }
    Err(anyhow!("meta CAS failed after retries"))
}

/// Load namespace documents: WAL replay when `meta.json` exists, else legacy manifest path.
pub async fn load_namespace(
    client: &Client,
    bucket: &str,
    namespace: &str,
) -> Result<LoadedNamespaceData> {
    let meta_key = meta_key(namespace);
    if let Some((meta, etag)) = get_meta(client, bucket, &meta_key).await? {
        let docs = replay_wal(client, bucket, namespace, &meta).await?;
        return Ok(LoadedNamespaceData {
            docs,
            meta_etag: etag,
        });
    }

    load_legacy(client, bucket, namespace).await
}

async fn replay_wal(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
) -> Result<HashMap<String, Document>> {
    let mut docs = HashMap::new();
    if meta.wal_commit_seq == 0 {
        return Ok(docs);
    }
    for seq in 1..=meta.wal_commit_seq {
        let key = wal_key(namespace, seq);
        let bytes = get_object_bytes(client, bucket, &key)
            .await
            .with_context(|| format!("read wal {seq:08}"))?;
        let entry = decode(&bytes)?;
        apply_entry(&mut docs, &entry)?;
    }
    Ok(docs)
}

async fn load_legacy(
    client: &Client,
    bucket: &str,
    namespace: &str,
) -> Result<LoadedNamespaceData> {
    let manifest_key = models::manifest_key(namespace);
    let manifest = match get_meta_json::<Manifest>(client, bucket, &manifest_key).await? {
        Some((m, _)) => m,
        None => {
            return Ok(LoadedNamespaceData {
                docs: HashMap::new(),
                meta_etag: None,
            });
        }
    };

    let mut docs = HashMap::new();
    for id in &manifest.doc_ids {
        let doc_key = models::doc_key(namespace, id);
        if let Some(bytes) = get_object_bytes_optional(client, bucket, &doc_key).await? {
            let doc: Document = serde_json::from_slice(&bytes).context("parse legacy document")?;
            docs.insert(id.clone(), doc);
        }
    }
    Ok(LoadedNamespaceData {
        docs,
        meta_etag: None,
    })
}

async fn get_meta(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<Option<(NamespaceMeta, Option<String>)>> {
    get_meta_json(client, bucket, key).await
}

async fn get_meta_json<T: serde::de::DeserializeOwned>(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<Option<(T, Option<String>)>> {
    let out = client.get_object().bucket(bucket).key(key).send().await;
    match out {
        Ok(resp) => {
            let etag = resp.e_tag().map(|s| s.to_string());
            let bytes = resp
                .body
                .collect()
                .await
                .context("read object body")?
                .into_bytes();
            let value: T = serde_json::from_slice(&bytes).context("parse json object")?;
            Ok(Some((value, etag)))
        }
        Err(e) => {
            let service = e.into_service_error();
            if service.is_no_such_key() {
                Ok(None)
            } else if service.meta().code() == Some("NoSuchBucket") {
                Err(anyhow!("S3 bucket not found"))
            } else {
                Err(anyhow!("get object {key}: {service}"))
            }
        }
    }
}

async fn get_object_bytes(client: &Client, bucket: &str, key: &str) -> Result<Vec<u8>> {
    get_object_bytes_optional(client, bucket, key)
        .await?
        .ok_or_else(|| anyhow!("object not found: {key}"))
}

async fn get_object_bytes_optional(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<Option<Vec<u8>>> {
    let out = client.get_object().bucket(bucket).key(key).send().await;
    match out {
        Ok(resp) => {
            let bytes = resp
                .body
                .collect()
                .await
                .context("read object body")?
                .into_bytes();
            Ok(Some(bytes.to_vec()))
        }
        Err(e) => {
            let service = e.into_service_error();
            if service.is_no_such_key() {
                Ok(None)
            } else {
                Err(anyhow!("get object {key}: {service}"))
            }
        }
    }
}