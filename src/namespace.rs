//! Per-namespace WAL append and replay on S3.

use crate::commit_lock::namespace_commit_lock;
use crate::meta::{
    meta_after_wal_commit_options, meta_key, next_wal_seq, DistanceMetric, NamespaceMeta,
    META_RETRIES,
};
use serde_json::Value;
use crate::models::Document;
use crate::wal::{
    apply_entry, decode_segment, decode_segment_with_policy, decode_snapshot, encode, wal_key,
    WalCorruptPolicy, WalEntry, WalSnapshot,
};
use crate::wal_compaction::wal_replay_from;
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

/// Fetch and decode one WAL segment.
pub async fn read_wal_entry(
    client: &Client,
    bucket: &str,
    namespace: &str,
    seq: u64,
) -> Result<WalEntry> {
    let bytes = read_wal_segment(client, bucket, namespace, seq).await?;
    decode_segment(&bytes, Some(seq)).context("decode wal entry")
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
        entries.push(read_wal_entry(client, bucket, namespace, seq).await?);
    }
    Ok(entries)
}

/// Load `wal/snapshot.bin` if present.
pub async fn read_wal_snapshot(
    client: &Client,
    bucket: &str,
    namespace: &str,
) -> Result<Option<WalSnapshot>> {
    let key = WalSnapshot::key(namespace);
    let Some((bytes, _)) = get_object_bytes_optional(client, bucket, &key).await? else {
        return Ok(None);
    };
    Ok(Some(decode_snapshot(&bytes)?))
}

/// Reconstruct the document map at `meta.wal_commit_seq` (snapshot baseline + WAL tail).
pub async fn load_docs_at_wal_commit(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
) -> Result<(HashMap<String, Document>, u64)> {
    let mut docs = HashMap::new();
    let mut last_applied = 0u64;

    if meta.wal_snapshot_seq > 0 {
        if let Some(snap) = read_wal_snapshot(client, bucket, namespace).await? {
            if snap.seq == meta.wal_snapshot_seq {
                last_applied = snap.seq;
                docs = snap.into_docs();
            } else {
                tracing::warn!(
                    "wal snapshot seq {} != meta.wal_snapshot_seq {} for {namespace}",
                    snap.seq,
                    meta.wal_snapshot_seq
                );
            }
        }
    }

    if let Some(from) = wal_replay_from(last_applied, meta.wal_commit_seq) {
        replay_wal_range(
            client,
            bucket,
            namespace,
            &mut docs,
            from,
            meta.wal_commit_seq,
        )
        .await?;
        last_applied = meta.wal_commit_seq;
    }

    Ok((docs, last_applied))
}

/// Collect all documents up to `index_cursor` (snapshot baseline + WAL replay).
pub async fn docs_at_index_cursor(
    client: &Client,
    bucket: &str,
    namespace: &str,
    index_cursor: u64,
) -> Result<HashMap<String, Document>> {
    if index_cursor == 0 {
        return Ok(HashMap::new());
    }

    let mut docs = HashMap::new();
    let mut replay_from = 1u64;

    if let Some(snap) = read_wal_snapshot(client, bucket, namespace).await? {
        if snap.seq <= index_cursor {
            replay_from = snap.seq.saturating_add(1);
            docs = snap.into_docs();
        }
    }

    if replay_from <= index_cursor {
        replay_wal_range(
            client,
            bucket,
            namespace,
            &mut docs,
            replay_from,
            index_cursor,
        )
        .await?;
    }
    Ok(docs)
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
    let policy = WalCorruptPolicy::current();
    for seq in from_seq..=to_seq {
        let bytes = read_wal_segment(client, bucket, namespace, seq).await?;
        if let Some(entry) = decode_segment_with_policy(&bytes, seq, policy)? {
            apply_entry(docs, &entry)?;
        }
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
    entry: WalEntry,
    schema_patch: Option<&Value>,
    distance_metric: Option<DistanceMetric>,
) -> Result<u64> {
    let commit_lock = namespace_commit_lock(namespace).await;
    let _commit = commit_lock.lock().await;

    for attempt in 0..META_RETRIES {
        let key = meta_key(namespace);
        let (meta, meta_etag) = match get_meta_with_retry(client, bucket, &key).await? {
            Some((m, e)) => (m, e),
            None => (NamespaceMeta::default(), None),
        };

        let seq = next_wal_seq(&meta);
        let wal_body = encode(&entry)?;
        put_wal_with_retry(client, bucket, namespace, seq, wal_body).await?;

        let next_meta =
            meta_after_wal_commit_options(&meta, seq, schema_patch, distance_metric)?;
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
                    let wal_object = wal_key(namespace, seq);
                    let _ = client
                        .delete_object()
                        .bucket(bucket)
                        .key(&wal_object)
                        .send()
                        .await;
                    tokio::time::sleep(Duration::from_millis(50 * (attempt as u64 + 1))).await;
                    continue;
                }
                return Err(anyhow!("put meta after wal: {service}"));
            }
        }
    }
    Err(anyhow!("meta CAS failed after retries"))
}

async fn put_wal_with_retry(
    client: &Client,
    bucket: &str,
    namespace: &str,
    seq: u64,
    wal_body: Vec<u8>,
) -> Result<()> {
    const PUT_RETRIES: u32 = 4;
    let key = wal_key(namespace, seq);
    let mut last_err = None;
    for attempt in 0..PUT_RETRIES {
        match client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from(wal_body.clone()))
            .send()
            .await
        {
            Ok(_) => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 < PUT_RETRIES {
                    tokio::time::sleep(Duration::from_millis(50 * (attempt as u64 + 1))).await;
                }
            }
        }
    }
    Err(last_err
        .map(|e| e.into_service_error())
        .map(|s| anyhow!("put wal segment {seq:08}: {s}"))
        .unwrap_or_else(|| anyhow!("put wal segment {seq:08}")))
}

async fn get_meta(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<Option<(NamespaceMeta, Option<String>)>> {
    get_meta_with_retry(client, bucket, key).await
}

async fn get_meta_with_retry(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<Option<(NamespaceMeta, Option<String>)>> {
    const GET_RETRIES: u32 = 4;
    let mut last_err = None;
    for attempt in 0..GET_RETRIES {
        match get_meta_json::<NamespaceMeta>(client, bucket, key).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 < GET_RETRIES {
                    tokio::time::sleep(Duration::from_millis(25 * (attempt as u64 + 1))).await;
                }
            }
        }
    }
    Err(last_err.unwrap())
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
            let code = service.meta().code();
            if service.is_no_such_key()
                || code == Some("NoSuchKey")
                || code == Some("NotFound")
            {
                Ok(None)
            } else if code == Some("NoSuchBucket") {
                Err(anyhow!("S3 bucket not found"))
            } else {
                let msg = format!("{service}");
                // MinIO sometimes returns a generic "unhandled error" on concurrent HEAD/GET
                // against a not-yet-created `meta.json`; treat as absent namespace.
                if key.ends_with("/meta.json")
                    && (msg.contains("unhandled error") || msg.contains("NotFound"))
                {
                    Ok(None)
                } else {
                    Err(anyhow!("get object {key}: {service}"))
                }
            }
        }
    }
}

async fn get_object_bytes(client: &Client, bucket: &str, key: &str) -> Result<Vec<u8>> {
    get_object_bytes_optional(client, bucket, key)
        .await?
        .map(|(b, _)| b)
        .ok_or_else(|| anyhow!("object not found: {key}"))
}

/// Fetch an S3 object as raw bytes, returning `None` when the key does not exist.
///
/// Returns `(bytes, etag)` on success so callers that need the ETag (e.g. cache
/// validation) can use the same helper. Callers that only need bytes destructure
/// with `(b, _)`.
pub async fn get_object_bytes_optional(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<Option<(Vec<u8>, Option<String>)>> {
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
            Ok(Some((bytes.to_vec(), etag)))
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
    use crate::wal::{apply_entry, decode_segment_with_policy, encode, WalCorruptPolicy, WalEntry};
    use crate::models::Document;

    #[test]
    fn replay_wal_range_applies_incremental_entries() {
        let e1 = WalEntry::from_write(
            vec![Document {
                id: "x".into(),
                attributes: Default::default(),
            }],
            vec![],
            vec![],
        )
        .unwrap();
        let e2 = WalEntry::from_write(vec![], vec![], vec!["x".into()]).unwrap();

        let mut docs = HashMap::new();
        apply_entry(&mut docs, &e1).unwrap();
        assert!(docs.contains_key("x"));
        apply_entry(&mut docs, &e2).unwrap();
        assert!(!docs.contains_key("x"));

        // Incremental path only fetches new segments; logic mirrors replay_wal_range loop.
        let bytes1 = encode(&e1).unwrap();
        let bytes2 = encode(&e2).unwrap();
        let mut docs2 = HashMap::new();
        for (seq, bytes) in [(1u64, bytes1), (2, bytes2)] {
            let entry = decode_segment_with_policy(&bytes, seq, WalCorruptPolicy::Fail)
                .unwrap()
                .unwrap();
            apply_entry(&mut docs2, &entry).unwrap();
        }
        assert!(docs2.is_empty());
    }
}