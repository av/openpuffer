//! Deep health probes against the configured S3 bucket (HeadBucket + read path).

use crate::meta::meta_key;
use crate::models::ROOT_PREFIX;
use anyhow::{Context, Result};
use aws_sdk_s3::Client;

/// Verify S3 connectivity: bucket exists and the `openpuffer/` prefix is readable.
///
/// When at least one namespace exists, also HEADs that namespace's `meta.json` (canary read).
pub async fn probe_s3_storage(client: &Client, bucket: &str) -> Result<()> {
    client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .context("head bucket")?;

    let out = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(ROOT_PREFIX)
        .delimiter("/")
        .max_keys(10)
        .send()
        .await
        .context("list openpuffer namespaces")?;

    if let Some(cp) = out.common_prefixes().first() {
        let prefix = cp.prefix().context("namespace common prefix")?;
        let name = prefix
            .strip_prefix(ROOT_PREFIX)
            .and_then(|s| s.strip_suffix('/'))
            .filter(|s| !s.is_empty())
            .context("parse namespace from prefix")?;
        let key = meta_key(name);
        client
            .head_object()
            .bucket(bucket)
            .key(&key)
            .send()
            .await
            .with_context(|| format!("head canary meta {key}"))?;
    }

    Ok(())
}