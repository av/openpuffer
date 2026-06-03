//! Local disk cache for S3 index segments (turbopuffer NVMe cache analogue).
//!
//! Layout: `{cache_dir}/{bucket}/{s3_key}` with sibling `{path}.etag` for validation.
//! Queries prefer cached bytes after a matching HEAD; cold misses GET from S3 then populate cache.

use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;


/// Disk cache for index objects (`index/fts-*`, `filter-*`, `centroids-l0/l1-*`, `clusters-*`).
#[derive(Debug)]
pub struct SegmentCache {
    root: Option<PathBuf>,
    s3_gets: AtomicU64,
}

impl SegmentCache {
    /// Memory-only mode (no disk reads/writes).
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            root: None,
            s3_gets: AtomicU64::new(0),
        })
    }

    pub fn new(root: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            root: Some(root),
            s3_gets: AtomicU64::new(0),
        })
    }

    pub fn from_optional(root: Option<PathBuf>) -> Arc<Self> {
        match root {
            Some(p) if !p.as_os_str().is_empty() => Self::new(p),
            _ => Self::disabled(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.root.is_some()
    }

    pub fn root(&self) -> Option<&Path> {
        self.root.as_deref()
    }

    /// Count of S3 `GetObject` calls (for tests).
    pub fn s3_get_count(&self) -> u64 {
        self.s3_gets.load(Ordering::SeqCst)
    }

    /// Reset GetObject counter (integration / warm-cache tests).
    pub fn reset_s3_get_count(&self) {
        self.s3_gets.store(0, Ordering::SeqCst);
    }

    fn local_path(&self, bucket: &str, s3_key: &str) -> Option<PathBuf> {
        let root = self.root.as_ref()?;
        Some(root.join(bucket).join(s3_key))
    }

    fn etag_sidecar(path: &Path) -> PathBuf {
        PathBuf::from(format!("{}.etag", path.display()))
    }

    fn normalize_etag(etag: &str) -> String {
        etag.trim_matches('"').to_string()
    }

    fn read_local(&self, bucket: &str, s3_key: &str) -> Option<(Vec<u8>, String)> {
        let path = self.local_path(bucket, s3_key)?;
        let bytes = std::fs::read(&path).ok()?;
        let etag_raw = std::fs::read_to_string(Self::etag_sidecar(&path)).ok()?;
        let etag = Self::normalize_etag(etag_raw.trim());
        if etag.is_empty() {
            return None;
        }
        Some((bytes, etag))
    }

    /// Write bytes + etag to disk (best-effort; errors logged at debug).
    pub fn write_local(&self, bucket: &str, s3_key: &str, bytes: &[u8], etag: &str) {
        let Some(path) = self.local_path(bucket, s3_key) else {
            return;
        };
        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::debug!("cache mkdir {}: {e:#}", parent.display());
                return;
            }
        }
        if let Err(e) = std::fs::write(&path, bytes) {
            tracing::debug!("cache write {}: {e:#}", path.display());
            return;
        }
        let etag_norm = Self::normalize_etag(etag);
        if let Err(e) = std::fs::write(Self::etag_sidecar(&path), &etag_norm) {
            tracing::debug!("cache etag {}: {e:#}", path.display());
        }
    }

    /// True when local bytes exist and etag matches `remote_etag` (no S3 GET needed).
    pub fn local_matches_etag(
        local: Option<(Vec<u8>, String)>,
        remote_etag: Option<&str>,
    ) -> Option<Vec<u8>> {
        let (bytes, local_etag) = local?;
        let remote = Self::normalize_etag(remote_etag?);
        if local_etag == remote {
            Some(bytes)
        } else {
            None
        }
    }

    async fn head_etag(client: &Client, bucket: &str, key: &str) -> Result<Option<String>> {
        let out = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        match out {
            Ok(resp) => Ok(resp.e_tag().map(|s| Self::normalize_etag(s))),
            Err(e) => {
                let service = e.into_service_error();
                if service.is_not_found() {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!("head object {key}: {service}"))
                }
            }
        }
    }

    async fn get_object_bytes(
        client: &Client,
        bucket: &str,
        key: &str,
    ) -> Result<Option<(Vec<u8>, Option<String>)>> {
        let out = client.get_object().bucket(bucket).key(key).send().await;
        match out {
            Ok(resp) => {
                let etag = resp.e_tag().map(|s| Self::normalize_etag(s));
                let bytes = resp
                    .body
                    .collect()
                    .await
                    .context("read object body")?
                    .into_bytes()
                    .to_vec();
                Ok(Some((bytes, etag)))
            }
            Err(e) => {
                let service = e.into_service_error();
                if service.is_no_such_key() {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!("get object {key}: {service}"))
                }
            }
        }
    }

    /// Fetch index object: cache hit (HEAD etag match) avoids GetObject; miss fetches S3 and populates cache.
    pub async fn get_bytes(
        self: &Arc<Self>,
        client: &Client,
        bucket: &str,
        s3_key: &str,
    ) -> Result<Option<Vec<u8>>> {
        if !self.enabled() {
            let got = Self::get_object_bytes(client, bucket, s3_key).await?;
            if got.is_some() {
                self.s3_gets.fetch_add(1, Ordering::SeqCst);
            }
            return Ok(got.map(|(b, _)| b));
        }

        let local = self.read_local(bucket, s3_key);
        if let Some(remote_etag) = Self::head_etag(client, bucket, s3_key).await? {
            if let Some(bytes) = Self::local_matches_etag(local, Some(&remote_etag)) {
                return Ok(Some(bytes));
            }
            // Stale or missing local — fall through to GET.
        } else {
            return Ok(None);
        }

        let got = Self::get_object_bytes(client, bucket, s3_key).await?;
        if let Some((bytes, etag)) = &got {
            self.s3_gets.fetch_add(1, Ordering::SeqCst);
            if let Some(etag) = etag {
                self.write_local(bucket, s3_key, bytes, etag);
            }
        }
        Ok(got.map(|(b, _)| b))
    }

    /// After indexer PUT, mirror object into cache using response etag.
    pub fn populate_after_put(
        self: &Arc<Self>,
        bucket: &str,
        s3_key: &str,
        bytes: &[u8],
        etag: Option<&str>,
    ) {
        if let Some(etag) = etag {
            self.write_local(bucket, s3_key, bytes, etag);
        }
    }

    /// Drop cached index segments for a namespace (after copy/delete).
    pub fn invalidate_namespace(&self, bucket: &str, namespace: &str) {
        let Some(root) = self.root.as_ref() else {
            return;
        };
        let dir = root.join(bucket).join(crate::models::ROOT_PREFIX).join(namespace);
        if dir.exists() {
            if let Err(e) = std::fs::remove_dir_all(&dir) {
                tracing::debug!("invalidate cache {dir:?}: {e}");
            }
        }
    }

    /// Background prefetch of index keys (non-blocking).
    pub fn prefetch_background(
        self: Arc<Self>,
        client: Client,
        bucket: String,
        keys: Vec<String>,
    ) {
        if !self.enabled() || keys.is_empty() {
            return;
        }
        tokio::spawn(async move {
            for key in keys {
                if let Err(e) = self.get_bytes(&client, &bucket, &key).await {
                    tracing::debug!("prefetch {key}: {e:#}");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_path_under_bucket_and_key() {
        let dir = tempfile::tempdir().unwrap();
        let cache = SegmentCache::new(dir.path().to_path_buf());
        let p = cache
            .local_path("mybucket", "openpuffer/ns/index/fts-00000001.bin")
            .unwrap();
        assert!(p.ends_with("mybucket/openpuffer/ns/index/fts-00000001.bin"));
    }

    #[test]
    fn write_and_read_local_with_etag() {
        let dir = tempfile::tempdir().unwrap();
        let cache = SegmentCache::new(dir.path().to_path_buf());
        let key = "openpuffer/ns/index/fts-00000001.bin";
        cache.write_local("bkt", key, b"payload", "\"abc123\"");
        let (bytes, etag) = cache.read_local("bkt", key).unwrap();
        assert_eq!(bytes, b"payload");
        assert_eq!(etag, "abc123");
    }

    #[test]
    fn local_matches_etag_returns_bytes_on_hit() {
        let hit = SegmentCache::local_matches_etag(
            Some((vec![1, 2, 3], "etag1".into())),
            Some("\"etag1\""),
        );
        assert_eq!(hit, Some(vec![1, 2, 3]));
    }

    #[test]
    fn local_matches_etag_miss_on_stale() {
        let miss = SegmentCache::local_matches_etag(
            Some((vec![1], "old".into())),
            Some("new"),
        );
        assert!(miss.is_none());
    }

    #[test]
    fn cache_hit_avoids_s3_get_counter_without_remote_fetch() {
        let dir = tempfile::tempdir().unwrap();
        let cache = SegmentCache::new(dir.path().to_path_buf());
        let key = "openpuffer/ns/index/filter-00000002.bin";
        cache.write_local("bkt", key, b"filter-bytes", "deadbeef");
        let local = cache.read_local("bkt", key);
        let hit = SegmentCache::local_matches_etag(local, Some("deadbeef"));
        assert_eq!(hit.as_deref(), Some(b"filter-bytes".as_slice()));
        assert_eq!(cache.s3_get_count(), 0);
    }

    #[test]
    fn disabled_cache_has_no_root() {
        let cache = SegmentCache::disabled();
        assert!(!cache.enabled());
        assert!(cache.local_path("b", "k").is_none());
    }

    #[tokio::test]
    async fn from_optional_empty_is_disabled() {
        let cache = SegmentCache::from_optional(Some(PathBuf::from("")));
        assert!(!cache.enabled());
    }
}