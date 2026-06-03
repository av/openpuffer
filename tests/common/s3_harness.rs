//! S3 integration harness: MinIO testcontainers or external endpoint via env.

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use openpuffer::meta::{meta_key, NamespaceMeta};
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
use tokio::time::sleep;

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
    let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .load()
        .await;
    let conf = aws_sdk_s3::config::Builder::from(&shared)
        .endpoint_url(endpoint)
        .force_path_style(true)
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

pub async fn fetch_meta_from_s3(
    client: &Client,
    bucket: &str,
    namespace: &str,
) -> NamespaceMeta {
    let key = meta_key(namespace);
    let bytes = get_object_bytes(client, bucket, &key).await;
    serde_json::from_slice(&bytes).expect("parse NamespaceMeta")
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

pub async fn list_keys_with_prefix(client: &Client, bucket: &str, prefix: &str) -> Vec<String> {
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
        let out = req.send().await.expect("list objects");
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
    keys
}

pub fn wal_upsert_ids(entry: &WalEntry) -> Vec<String> {
    entry.upserts.iter().map(|u| u.id.clone()).collect()
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
    let keys = list_keys_with_prefix(client, bucket, &index_prefix).await;
    let has_fts = keys.iter().any(|k| k.contains("/index/fts-") && k.ends_with(".bin"));
    let has_centroids = keys.iter().any(|k| k.ends_with("/index/centroids-l0.bin"));
    let has_filter = keys
        .iter()
        .any(|k| k.contains("/index/filter-") && k.ends_with(".bin"));
    assert!(has_fts, "expected index/fts-*.bin, keys={keys:?}");
    assert!(has_centroids, "expected index/centroids-l0.bin, keys={keys:?}");
    assert!(has_filter, "expected index/filter-*.bin, keys={keys:?}");
}

pub async fn assert_two_level_centroids_on_backend(
    client: &Client,
    bucket: &str,
    namespace: &str,
) {
    let index_prefix = format!("{ROOT_PREFIX}{namespace}/index/");
    let keys = list_keys_with_prefix(client, bucket, &index_prefix).await;
    let l0_key = keys
        .iter()
        .find(|k| k.ends_with("/index/centroids-l0.bin"))
        .expect("centroids-l0.bin missing");
    assert!(
        object_size(client, bucket, l0_key).await > 0,
        "centroids-l0.bin must be non-empty"
    );

    let l1_keys: Vec<_> = keys
        .iter()
        .filter(|k| k.contains("/index/centroids-l1-") && k.ends_with(".bin"))
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
    std::env::var_os("CARGO_BIN_EXE_openpuffer")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/debug/openpuffer")
        })
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
            if let Ok(resp) = client
                .get(format!("{}/health", self.base_url))
                .send()
                .await
            {
                if resp.status() == StatusCode::OK {
                    return;
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
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "index_cursor never caught up for {namespace} within {timeout:?}"
            );
        }
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status() == StatusCode::OK {
                let v: Value = resp.json().await.expect("metadata json");
                let cursor = v["index_cursor"].as_u64().unwrap_or(0);
                let commit = v["wal_commit_seq"].as_u64().unwrap_or(0);
                if commit > 0 && cursor == commit {
                    return;
                }
            }
        }
        sleep(Duration::from_millis(250)).await;
    }
}

pub async fn upsert_batch(base_url: &str, namespace: &str, rows: Value) {
    write_batch(base_url, namespace, json!({ "upsert_rows": rows })).await;
}

pub async fn write_batch(base_url: &str, namespace: &str, body: Value) {
    let resp = reqwest::Client::new()
        .post(format!("{base_url}/v2/namespaces/{namespace}"))
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
        .post(format!("{base_url}/v2/namespaces/{namespace}/query"))
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