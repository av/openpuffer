//! S3 integration harness: MinIO testcontainers or external endpoint via env.
//!
//! WAL helpers (`decode_wal_entry_from_s3`, `list_wal_keys`, `wal_segment_seqs`) back
//! integration tests that assert bincode `WalEntry` bytes on S3 (conditional upserts,
//! `patch_by_filter`, branch/copy key parity).

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use openpuffer::index::vector::CentroidIndexL0;
use openpuffer::meta::{effective_vector_fields, meta_key, NamespaceMeta};
use openpuffer::models::ROOT_PREFIX;
use openpuffer::wal::{decode, decode_snapshot, wal_key, WalEntry, WalSnapshot};
use reqwest::StatusCode;
use serde_json::{json, Value};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::minio::MinIO;
use tokio::time::{sleep, Instant};

/// Percent-encode a namespace for use as a single URL path segment (e.g. `bad/name` → `bad%2Fname`).
pub fn namespace_path_segment(name: &str) -> String {
    urlencoding::encode(name).into_owned()
}

pub const MINIO_USER: &str = "minioadmin";
pub const MINIO_PASSWORD: &str = "minioadmin";
pub const DEFAULT_BUCKET: &str = "openpuffer-integration";

enum S3Keepalive {
    Minio(ContainerAsync<MinIO>),
    External,
}

/// MinIO via testcontainers or an external S3-compatible endpoint (`OPENPUFFER_TEST_S3_*`).
pub struct S3Fixture {
    pub client: Client,
    pub bucket: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    _keepalive: S3Keepalive,
}

impl S3Fixture {
    /// Start MinIO in Docker (testcontainers).
    pub async fn from_testcontainers() -> Self {
        let minio = MinIO::default().start().await.expect("start MinIO container");
        let host = minio.get_host().await.expect("minio host");
        let port = minio
            .get_host_port_ipv4(9000)
            .await
            .expect("minio api port");
        let endpoint = format!("http://{host}:{port}");
        let client = s3_client(&endpoint, MINIO_USER, MINIO_PASSWORD).await;
        let bucket = DEFAULT_BUCKET.to_string();
        ensure_bucket(&client, &bucket).await;
        Self {
            client,
            bucket,
            endpoint,
            access_key: MINIO_USER.to_string(),
            secret_key: MINIO_PASSWORD.to_string(),
            _keepalive: S3Keepalive::Minio(minio),
        }
    }

    /// External endpoint when `OPENPUFFER_TEST_S3_ENDPOINT` is set.
    pub async fn from_env() -> Option<Self> {
        let endpoint = std::env::var("OPENPUFFER_TEST_S3_ENDPOINT").ok()?;
        let bucket = std::env::var("OPENPUFFER_TEST_S3_BUCKET")
            .unwrap_or_else(|_| DEFAULT_BUCKET.to_string());
        let access_key = std::env::var("OPENPUFFER_TEST_S3_ACCESS_KEY")
            .unwrap_or_else(|_| MINIO_USER.to_string());
        let secret_key = std::env::var("OPENPUFFER_TEST_S3_SECRET_KEY")
            .unwrap_or_else(|_| MINIO_PASSWORD.to_string());
        let client = s3_client(&endpoint, &access_key, &secret_key).await;
        ensure_bucket(&client, &bucket).await;
        Some(Self {
            client,
            bucket,
            endpoint,
            access_key,
            secret_key,
            _keepalive: S3Keepalive::External,
        })
    }
}

/// Alias for [`S3Fixture::from_env`].
pub async fn s3_fixture_from_env() -> Option<S3Fixture> {
    S3Fixture::from_env().await
}

pub async fn s3_client(endpoint: &str, access_key: &str, secret_key: &str) -> Client {
    let creds = Credentials::new(access_key, secret_key, None, None, "integration-test");
    let http = openpuffer::config::shared_s3_http_client();
    let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .http_client(http.clone())
        .load()
        .await;
    let conf = aws_sdk_s3::config::Builder::from(&shared)
        .endpoint_url(endpoint)
        .force_path_style(true)
        .http_client(http)
        .build();
    Client::from_conf(conf)
}

pub async fn ensure_bucket(client: &Client, bucket: &str) {
    let _ = client.create_bucket().bucket(bucket).send().await;
}

pub async fn assert_key_exists(client: &Client, bucket: &str, key: &str) {
    assert!(
        s3_object_exists(client, bucket, key).await,
        "expected S3 object {key}"
    );
}

pub async fn get_object_bytes(client: &Client, bucket: &str, key: &str) -> Vec<u8> {
    let out = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap_or_else(|e| panic!("get object {key}: {e}"));
    out.body
        .collect()
        .await
        .expect("read object body")
        .into_bytes()
        .to_vec()
}

pub async fn head_object_etag(client: &Client, bucket: &str, key: &str) -> String {
    let out = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap_or_else(|e| panic!("head object {key}: {e}"));
    out.e_tag()
        .map(|s| s.to_string())
        .unwrap_or_else(|| panic!("S3 object {key} missing ETag"))
}

pub async fn object_size(client: &Client, bucket: &str, key: &str) -> u64 {
    let out = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap_or_else(|e| panic!("head object {key}: {e}"));
    out.content_length().unwrap_or(0) as u64
}

pub async fn decode_wal_snapshot_from_s3(
    client: &Client,
    bucket: &str,
    namespace: &str,
) -> WalSnapshot {
    let key = WalSnapshot::key(namespace);
    let bytes = get_object_bytes(client, bucket, &key).await;
    decode_snapshot(&bytes).expect("decode WalSnapshot from S3 bytes")
}

pub async fn decode_wal_entry_from_s3(
    client: &Client,
    bucket: &str,
    namespace: &str,
    seq: u64,
) -> WalEntry {
    let key = wal_key(namespace, seq);
    let bytes = get_object_bytes(client, bucket, &key).await;
    decode(&bytes).expect("decode WalEntry from S3 bytes")
}

/// Flip the CRC32 trailer byte of `wal/{seq:08}.bin` on S3 (v1 wire format must decode before call).
pub async fn corrupt_wal_crc_byte_on_s3(
    client: &Client,
    bucket: &str,
    namespace: &str,
    seq: u64,
) {
    let key = wal_key(namespace, seq);
    let mut bytes = get_object_bytes(client, bucket, &key).await;
    assert!(
        !bytes.is_empty(),
        "wal segment {seq:08} empty before corruption"
    );
    decode(&bytes).expect("wal must be valid before corruption");
    let tail = bytes.len() - 1;
    bytes[tail] ^= 0xFF;
    openpuffer::wal::decode(&bytes).expect_err("corrupted wal must fail CRC decode");
    client
        .put_object()
        .bucket(bucket)
        .key(&key)
        .body(ByteStream::from(bytes))
        .send()
        .await
        .unwrap_or_else(|e| panic!("put corrupted wal {key}: {e}"));
}

pub async fn fetch_meta_from_s3(
    client: &Client,
    bucket: &str,
    namespace: &str,
) -> NamespaceMeta {
    let key = meta_key(namespace);
    let bytes = get_object_bytes(client, bucket, &key).await;
    serde_json::from_slice(&bytes).expect("parse NamespaceMeta")
}

/// S3 key for L0 centroids (`index/{field}/centroids-l0.bin` or legacy `index/centroids-l0.bin`).
pub async fn centroids_l0_key_for_namespace(
    client: &Client,
    bucket: &str,
    namespace: &str,
) -> String {
    let meta = fetch_meta_from_s3(client, bucket, namespace).await;
    if let Some(cfg) = effective_vector_fields(&meta).first() {
        CentroidIndexL0::key(namespace, &cfg.name)
    } else {
        CentroidIndexL0::legacy_key(namespace)
    }
}

/// True if any centroids-l0.bin exists under `index/` (per-field or legacy layout).
pub fn index_has_centroids_l0(keys: &[String]) -> bool {
    keys.iter().any(|k| k.ends_with("centroids-l0.bin"))
}

/// Full S3 key for per-column L0 centroids (`openpuffer/{ns}/index/{field}/centroids-l0.bin`).
pub fn centroids_l0_s3_key(namespace: &str, field: &str) -> String {
    CentroidIndexL0::key(namespace, field)
}

/// Assert non-empty `centroids-l0.bin` exists for a named vector column.
pub async fn assert_centroids_l0_for_field(
    client: &Client,
    bucket: &str,
    namespace: &str,
    field: &str,
) {
    let key = centroids_l0_s3_key(namespace, field);
    assert_key_exists(client, bucket, &key).await;
    assert!(
        object_size(client, bucket, &key).await > 0,
        "centroids-l0 for field {field} must be non-empty at {key}"
    );
}

/// Assert `meta.json` on S3 records indexed vector columns (names + dimensions).
pub fn assert_meta_vector_fields(meta: &NamespaceMeta, expected: &[(&str, u32)]) {
    assert_eq!(
        meta.vector_fields.len(),
        expected.len(),
        "vector_fields in meta.json: {:?}",
        meta.vector_fields
    );
    for (name, dims) in expected {
        let cfg = meta
            .vector_fields
            .iter()
            .find(|f| f.name == *name)
            .unwrap_or_else(|| panic!("meta.vector_fields missing {name:?}"));
        assert_eq!(
            cfg.dimensions, *dims,
            "dimensions for {name} in meta"
        );
        assert!(
            cfg.segment_id > 0 || meta.index_cursor > 0,
            "field {name} should be indexed (segment_id={}, index_cursor={})",
            cfg.segment_id,
            meta.index_cursor
        );
    }
    for name in expected.iter().map(|(n, _)| *n) {
        assert!(
            meta.schema.get(name).is_some(),
            "meta.schema must declare vector field {name}: {:?}",
            meta.schema
        );
    }
}

pub async fn list_namespace_keys(client: &Client, bucket: &str, namespace: &str) -> Vec<String> {
    let prefix = format!("{ROOT_PREFIX}{namespace}/");
    list_keys_with_prefix(client, bucket, &prefix).await
}

pub async fn s3_object_exists(client: &Client, bucket: &str, key: &str) -> bool {
    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(_) => true,
        Err(e) => {
            let service = e.into_service_error();
            if service.is_not_found() {
                false
            } else {
                panic!("head object {key}: {service}");
            }
        }
    }
}

const LIST_OBJECTS_RETRY_ATTEMPTS: u32 = 5;

async fn list_keys_with_prefix_once(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix);
        if let Some(t) = &token {
            req = req.continuation_token(t);
        }
        let out = req
            .send()
            .await
            .map_err(|e| format!("list objects prefix={prefix}: {e}"))?;
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
    Ok(keys)
}

/// List S3 keys under `prefix`, retrying transient ListObjects failures (MinIO load).
pub async fn list_keys_with_prefix(client: &Client, bucket: &str, prefix: &str) -> Vec<String> {
    let mut last_err = String::new();
    for attempt in 0..LIST_OBJECTS_RETRY_ATTEMPTS {
        match list_keys_with_prefix_once(client, bucket, prefix).await {
            Ok(keys) => return keys,
            Err(e) => {
                last_err = e;
                sleep(Duration::from_millis(80 * (attempt as u64 + 1))).await;
            }
        }
    }
    panic!(
        "list objects failed after {LIST_OBJECTS_RETRY_ATTEMPTS} attempts for prefix={prefix}: {last_err}"
    );
}

/// Poll ListObjects until at least `min_count` keys appear (eventual listing consistency).
pub async fn list_keys_with_prefix_min_count(
    client: &Client,
    bucket: &str,
    prefix: &str,
    min_count: usize,
    timeout: Duration,
) -> Vec<String> {
    let deadline = Instant::now() + timeout;
    let mut last = Vec::new();
    loop {
        last = list_keys_with_prefix(client, bucket, prefix).await;
        if last.len() >= min_count {
            return last;
        }
        if Instant::now() >= deadline {
            panic!(
                "list prefix={prefix}: expected >={min_count} keys within {timeout:?}, last={last:?}"
            );
        }
        sleep(Duration::from_millis(200)).await;
    }
}

/// Poll ListObjects until `predicate` holds on the key set.
pub async fn list_keys_with_prefix_until<F>(
    client: &Client,
    bucket: &str,
    prefix: &str,
    timeout: Duration,
    mut predicate: F,
) -> Vec<String>
where
    F: FnMut(&[String]) -> bool,
{
    let deadline = Instant::now() + timeout;
    let mut last = Vec::new();
    loop {
        last = list_keys_with_prefix(client, bucket, prefix).await;
        if predicate(&last) {
            return last;
        }
        if Instant::now() >= deadline {
            panic!(
                "list prefix={prefix}: predicate not satisfied within {timeout:?}, last={last:?}"
            );
        }
        sleep(Duration::from_millis(200)).await;
    }
}

fn index_segment_keys_ready(keys: &[String]) -> bool {
    let has_fts = keys.iter().any(|k| k.contains("/index/fts-") && k.ends_with(".bin"));
    let has_centroids = keys.iter().any(|k| k.ends_with("centroids-l0.bin"));
    let has_filter = keys
        .iter()
        .any(|k| k.contains("/index/filter-") && k.ends_with(".bin"));
    has_fts && has_centroids && has_filter
}

fn two_level_centroid_keys_ready(keys: &[String]) -> bool {
    let has_l0 = keys.iter().any(|k| k.ends_with("centroids-l0.bin"));
    let has_l1 = keys
        .iter()
        .any(|k| k.contains("centroids-l1-") && k.ends_with(".bin"));
    has_l0 && has_l1
}

pub fn wal_upsert_ids(entry: &WalEntry) -> Vec<String> {
    entry.upserts.iter().map(|u| u.id.clone()).collect()
}

pub fn wal_patch_ids(entry: &WalEntry) -> Vec<String> {
    entry.patches.iter().map(|p| p.id.clone()).collect()
}

/// Sorted WAL segment sequence numbers from `wal/{seq:08}.bin` keys (excludes `snapshot.bin`).
pub fn wal_segment_seqs(keys: &[String]) -> Vec<u64> {
    let mut seqs: Vec<u64> = keys
        .iter()
        .filter_map(|k| {
            let name = k.rsplit('/').next()?;
            let stem = name.strip_suffix(".bin")?;
            if stem == "snapshot" {
                return None;
            }
            stem.parse().ok()
        })
        .collect();
    seqs.sort_unstable();
    seqs
}

pub async fn list_wal_keys(client: &Client, bucket: &str, namespace: &str) -> Vec<String> {
    let wal_prefix = format!("{ROOT_PREFIX}{namespace}/wal/");
    list_keys_with_prefix(client, bucket, &wal_prefix).await
}

pub async fn assert_wal_layout_after_write(
    client: &Client,
    bucket: &str,
    namespace: &str,
) {
    let meta = meta_key(namespace);
    assert_key_exists(client, bucket, &meta).await;
    let wal = wal_key(namespace, 1);
    assert_key_exists(client, bucket, &wal).await;

    let docs_prefix = format!("{ROOT_PREFIX}{namespace}/docs/");
    let legacy_keys: Vec<_> = list_keys_with_prefix(client, bucket, &docs_prefix)
        .await
        .into_iter()
        .filter(|k| k.ends_with(".json"))
        .collect();
    assert!(
        legacy_keys.is_empty(),
        "legacy docs/*.json must not exist, found {legacy_keys:?}"
    );
    let manifest = format!("{ROOT_PREFIX}{namespace}/manifest.json");
    assert!(
        !s3_object_exists(client, bucket, &manifest).await,
        "legacy manifest.json must not exist"
    );
}

pub async fn assert_index_objects(client: &Client, bucket: &str, namespace: &str) {
    let index_prefix = format!("{ROOT_PREFIX}{namespace}/index/");
    let keys = list_keys_with_prefix_until(
        client,
        bucket,
        &index_prefix,
        Duration::from_secs(45),
        index_segment_keys_ready,
    )
    .await;
    assert!(
        index_segment_keys_ready(&keys),
        "expected fts/centroids/filter index segments, keys={keys:?}"
    );
}

pub async fn assert_two_level_centroids_on_backend(
    client: &Client,
    bucket: &str,
    namespace: &str,
) {
    let index_prefix = format!("{ROOT_PREFIX}{namespace}/index/");
    let keys = list_keys_with_prefix_until(
        client,
        bucket,
        &index_prefix,
        Duration::from_secs(45),
        two_level_centroid_keys_ready,
    )
    .await;
    let l0_key = keys
        .iter()
        .find(|k| k.ends_with("centroids-l0.bin"))
        .expect("centroids-l0.bin missing");
    assert!(
        object_size(client, bucket, l0_key).await > 0,
        "centroids-l0.bin must be non-empty"
    );

    let l1_keys: Vec<_> = keys
        .iter()
        .filter(|k| k.contains("centroids-l1-") && k.ends_with(".bin"))
        .collect();
    assert!(
        !l1_keys.is_empty(),
        "expected centroids-l1-*.bin under index/, keys={keys:?}"
    );
    for l1 in &l1_keys {
        assert!(
            object_size(client, bucket, l1).await > 0,
            "centroids-l1 segment {l1} must be non-empty"
        );
    }
}

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

pub fn openpuffer_bin() -> PathBuf {
    if let Some(p) = std::env::var_os("CARGO_BIN_EXE_openpuffer") {
        return PathBuf::from(p);
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let release = manifest.join("target/release/openpuffer");
    if release.exists() {
        return release;
    }
    manifest.join("target/debug/openpuffer")
}

pub struct ServeHandle {
    pub child: Child,
    pub base_url: String,
}

impl ServeHandle {
    pub fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    pub fn spawn(fixture: &S3Fixture, listen: &str) -> Self {
        Self::spawn_with_cache(fixture, listen, None)
    }

    pub fn spawn_with_cache(
        fixture: &S3Fixture,
        listen: &str,
        cache_dir: Option<PathBuf>,
    ) -> Self {
        Self::spawn_with_options(fixture, listen, cache_dir, None, None)
    }

    pub fn spawn_with_options(
        fixture: &S3Fixture,
        listen: &str,
        cache_dir: Option<PathBuf>,
        write_max_batch_ops: Option<usize>,
        write_max_delay_ms: Option<u64>,
    ) -> Self {
        Self::spawn_with_limits(
            fixture,
            listen,
            cache_dir,
            write_max_batch_ops,
            write_max_delay_ms,
            None,
            None,
            None,
        )
    }

    pub fn spawn_with_limits(
        fixture: &S3Fixture,
        listen: &str,
        cache_dir: Option<PathBuf>,
        write_max_batch_ops: Option<usize>,
        write_max_delay_ms: Option<u64>,
        max_upsert_rows: Option<usize>,
        max_filter_batch_rows: Option<usize>,
        wal_corrupt_policy: Option<&str>,
    ) -> Self {
        Self::spawn_with_limits_and_ann_version(
            fixture,
            listen,
            cache_dir,
            write_max_batch_ops,
            write_max_delay_ms,
            max_upsert_rows,
            max_filter_batch_rows,
            wal_corrupt_policy,
            None,
        )
    }

    pub fn spawn_with_limits_and_ann_version(
        fixture: &S3Fixture,
        listen: &str,
        cache_dir: Option<PathBuf>,
        write_max_batch_ops: Option<usize>,
        write_max_delay_ms: Option<u64>,
        max_upsert_rows: Option<usize>,
        max_filter_batch_rows: Option<usize>,
        wal_corrupt_policy: Option<&str>,
        ann_version: Option<u8>,
    ) -> Self {
        let bin = openpuffer_bin();
        assert!(
            bin.exists(),
            "openpuffer binary not found at {}; run `cargo build --features integration` first",
            bin.display()
        );
        let mut args = vec![
            "serve".to_string(),
            "--listen".to_string(),
            listen.to_string(),
            "--s3-endpoint".to_string(),
            fixture.endpoint.clone(),
            "--s3-bucket".to_string(),
            fixture.bucket.clone(),
            "--s3-region".to_string(),
            "us-east-1".to_string(),
            "--s3-access-key".to_string(),
            fixture.access_key.clone(),
            "--s3-secret-key".to_string(),
            fixture.secret_key.clone(),
        ];
        if let Some(dir) = cache_dir {
            args.push("--cache-dir".to_string());
            args.push(dir.display().to_string());
        }
        if let Some(ops) = write_max_batch_ops {
            args.push("--write-max-batch-ops".to_string());
            args.push(ops.to_string());
        }
        if let Some(ms) = write_max_delay_ms {
            args.push("--write-max-delay-ms".to_string());
            args.push(ms.to_string());
        }
        if let Some(rows) = max_upsert_rows {
            args.push("--max-upsert-rows".to_string());
            args.push(rows.to_string());
        }
        if let Some(batch) = max_filter_batch_rows {
            args.push("--max-filter-batch-rows".to_string());
            args.push(batch.to_string());
        }
        if let Some(policy) = wal_corrupt_policy {
            args.push("--wal-corrupt-policy".to_string());
            args.push(policy.to_string());
        }
        if let Some(ver) = ann_version {
            args.push("--ann-version".to_string());
            args.push(ver.to_string());
        }
        let child = Command::new(&bin)
            .args(&args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn openpuffer serve");
        ServeHandle {
            child,
            base_url: format!("http://{listen}"),
        }
    }

    pub async fn wait_ready(&self) {
        let client = reqwest::Client::new();
        for _ in 0..60 {
            for path in ["/v1/ready", "/health"] {
                if let Ok(resp) = client
                    .get(format!("{}{}", self.base_url, path))
                    .send()
                    .await
                {
                    if resp.status() == StatusCode::OK {
                        return;
                    }
                }
            }
            sleep(Duration::from_millis(250)).await;
        }
        panic!("openpuffer serve did not become ready at {}", self.base_url);
    }
}

impl Drop for ServeHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub async fn wait_until_indexed(base_url: &str, namespace: &str, timeout: Duration) {
    let client = reqwest::Client::new();
    let url = format!("{base_url}/v1/namespaces/{namespace}");
    let deadline = Instant::now() + timeout;
    let mut last_cursor = 0u64;
    let mut last_commit = 0u64;
    let mut last_status = "no_response".to_string();
    loop {
        if Instant::now() >= deadline {
            panic!(
                "index_cursor never caught up for {namespace} within {timeout:?} \
                 (last cursor={last_cursor} commit={last_commit} status={last_status})"
            );
        }
        if let Ok(resp) = client.get(&url).send().await {
            last_status = format!("{}", resp.status());
            if resp.status() == StatusCode::OK {
                let v: Value = resp.json().await.expect("metadata json");
                last_cursor = v["index_cursor"].as_u64().unwrap_or(0);
                last_commit = v["wal_commit_seq"].as_u64().unwrap_or(0);
                if last_commit > 0 && last_cursor == last_commit {
                    return;
                }
            }
        } else {
            last_status = "request_error".to_string();
        }
        sleep(Duration::from_millis(250)).await;
    }
}

pub async fn upsert_batch(base_url: &str, namespace: &str, rows: Value) {
    write_batch(base_url, namespace, json!({ "upsert_rows": rows })).await;
}

pub async fn write_batch(base_url: &str, namespace: &str, body: Value) {
    let resp = reqwest::Client::new()
        .post(format!(
            "{base_url}/v2/namespaces/{}",
            namespace_path_segment(namespace)
        ))
        .json(&body)
        .send()
        .await
        .expect("write request");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "write failed: {}",
        resp.text().await.unwrap_or_default()
    );
}

pub async fn query_response_ns(base_url: &str, namespace: &str, body: Value) -> Value {
    let resp = reqwest::Client::new()
        .post(format!(
            "{base_url}/v2/namespaces/{}/query",
            namespace_path_segment(namespace)
        ))
        .json(&body)
        .send()
        .await
        .expect("query request");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "query failed: {}",
        resp.text().await.unwrap_or_default()
    );
    resp.json().await.expect("query json")
}

/// Assert each query row has turbopuffer `$dist` (from `QueryRow::dist`) as a finite number.
pub fn assert_rows_have_numeric_dist(rows: &Value) {
    let rows = rows.as_array().expect("rows array");
    assert!(!rows.is_empty(), "expected at least one row with $dist");
    for (i, row) in rows.iter().enumerate() {
        let dist = row
            .get("$dist")
            .and_then(|v| v.as_f64())
            .filter(|d| d.is_finite());
        assert!(
            dist.is_some(),
            "row {i} missing numeric $dist: {row}"
        );
        assert!(
            row.get("dist").is_none(),
            "JSON must use $dist not dist: {row}"
        );
    }
}

pub async fn query_ids_ns(
    base_url: &str,
    namespace: &str,
    rank_by: Value,
    filters: Option<Value>,
) -> Vec<String> {
    let mut body = json!({
        "rank_by": rank_by,
        "top_k": 3
    });
    if let Some(f) = filters {
        body["filters"] = f;
    }
    let v = query_response_ns(base_url, namespace, body).await;
    v["rows"]
        .as_array()
        .expect("rows array")
        .iter()
        .map(|r| r["id"].as_str().expect("row id").to_string())
        .collect()
}

pub async fn export_all_ids(
    base_url: &str,
    namespace: &str,
    page_limit: Option<usize>,
) -> Vec<String> {
    let client = reqwest::Client::new();
    let mut all = Vec::new();
    let mut last_id: Option<String> = None;
    loop {
        let mut url = format!("{base_url}/v1/namespaces/{namespace}/export");
        let mut sep = '?';
        if let Some(ref lid) = last_id {
            url.push_str(&format!("{sep}last_id={lid}"));
            sep = '&';
        }
        if let Some(limit) = page_limit {
            url.push_str(&format!("{sep}limit={limit}"));
        }
        let resp = client.get(&url).send().await.expect("export GET");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "export failed: {}",
            resp.text().await.unwrap_or_default()
        );
        let v: Value = resp.json().await.expect("export json");
        let commit = v["wal_commit_seq"].as_u64().unwrap_or(0);
        assert!(commit > 0, "export wal_commit_seq must be set");
        let rows = v["rows"].as_array().expect("export rows array");
        for row in rows {
            all.push(row["id"].as_str().expect("row id").to_string());
        }
        match v["next_last_id"].as_str() {
            Some(next) => last_id = Some(next.to_string()),
            None => break,
        }
        if rows.is_empty() {
            break;
        }
    }
    all.sort();
    all.dedup();
    all
}