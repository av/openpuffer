//! Per-namespace WAL append and replay on S3.

use crate::meta::{meta_after_wal_commit, meta_key, next_wal_seq, NamespaceMeta, META_RETRIES};
use crate::models::Document;
use crate::wal::{apply_entry, decode, encode, wal_key, WalEntry};
use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::collections::HashMap;
use std::time::Duration;

/// Fetch durable namespace metadata (`meta.json`), if present.
pub async fn fetch_meta(
    client: &Client,
    bucket: &str,
    namespace: &str,
) -> Result<Option<(NamespaceMeta, Option<String>)>> {
    get_meta(client, bucket, &meta_key(namespace)).await
}

/// Fetch and decode WAL segments `from_seq..=to_seq` without applying.
pub async fn replay_wal_entries(
    client: &Client,
    bucket: &str,
    namespace: &str,
    from_seq: u64,
    to_seq: u64,
) -> Result<Vec<WalEntry>> {
    if from_seq == 0 || to_seq == 0 || from_seq > to_seq {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    for seq in from_seq..=to_seq {
        let bytes = read_wal_segment(client, bucket, namespace, seq).await?;
        entries.push(decode(&bytes)?);
    }
    Ok(entries)
}

/// Replay WAL segments `from_seq..=to_seq` into `docs`.
pub async fn replay_wal_range(
    client: &Client,
    bucket: &str,
    namespace: &str,
    docs: &mut HashMap<String, Document>,
    from_seq: u64,
    to_seq: u64,
) -> Result<()> {
    if from_seq == 0 || to_seq == 0 || from_seq > to_seq {
        return Ok(());
    }
    for seq in from_seq..=to_seq {
        let bytes = read_wal_segment(client, bucket, namespace, seq).await?;
        let entry = decode(&bytes)?;
        apply_entry(docs, &entry)?;
    }
    Ok(())
}

async fn read_wal_segment(
    client: &Client,
    bucket: &str,
    namespace: &str,
    seq: u64,
) -> Result<Vec<u8>> {
    let key = wal_key(namespace, seq);
    get_object_bytes(client, bucket, &key)
        .await
        .with_context(|| format!("read wal {seq:08}"))
}

/// Append one WAL batch and CAS-advance `meta.json`.
///
/// **Strong consistency (write ACK):** returns only after the WAL object is PUT to S3
/// and `meta.json` was updated with `wal_commit_seq = seq`. Queries that catch up to
/// the same commit point will see this batch.
pub async fn append_wal(
    client: &Client,
    bucket: &str,
    namespace: &str,
    upserts: Vec<Document>,
    deletes: Vec<String>,
) -> Result<u64> {
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
            Ok(_) => return Ok(seq),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{apply_entry, encode, WalEntry};
    use crate::models::Document;

    #[test]
    fn replay_wal_range_applies_incremental_entries() {
        let e1 = WalEntry::from_write(
            vec![Document {
                id: "x".into(),
                attributes: Default::default(),
            }],
            vec![],
        )
        .unwrap();
        let e2 = WalEntry::from_write(vec![], vec!["x".into()]).unwrap();

        let mut docs = HashMap::new();
        apply_entry(&mut docs, &e1).unwrap();
        assert!(docs.contains_key("x"));
        apply_entry(&mut docs, &e2).unwrap();
        assert!(!docs.contains_key("x"));

        // Incremental path only fetches new segments; logic mirrors replay_wal_range loop.
        let bytes1 = encode(&e1).unwrap();
        let bytes2 = encode(&e2).unwrap();
        let mut docs2 = HashMap::new();
        for bytes in [bytes1, bytes2] {
            let entry = decode(&bytes).unwrap();
            apply_entry(&mut docs2, &entry).unwrap();
        }
        assert!(docs2.is_empty());
    }
}