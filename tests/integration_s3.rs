//! S3 round-trip integration tests against MinIO via testcontainers.
//!
//! Asserts turbopuffer-style layout (`meta.json`, `wal/`, `index/`), background indexing,
//! vector / FTS / hybrid / filter queries, and restart persistence — no `docs/{id}.json`.

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use openpuffer::meta::{meta_key, NamespaceMeta};
use openpuffer::models::ROOT_PREFIX;
use reqwest::StatusCode;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;
use tokio::time::sleep;

const MINIO_USER: &str = "minioadmin";
const MINIO_PASSWORD: &str = "minioadmin";
const BUCKET: &str = "openpuffer-integration";
const NAMESPACE: &str = "itest";
const NAMESPACE_INCR: &str = "itest-incr";
const NAMESPACE_WARM: &str = "itest-warm";
const NAMESPACE_DEL_FILTER: &str = "itest-del-filter";
const NAMESPACE_PATCH: &str = "itest-patch";
const NAMESPACE_CONCURRENT: &str = "itestconcurrent";
const NAMESPACE_RESTART_WRITE: &str = "itest-restart-write";
const NAMESPACE_EXPORT: &str = "itest-export";
const NAMESPACE_WAL_RATE: &str = "itest-wal-rate";
const NAMESPACE_COPY_SRC: &str = "itest-copy-src";
const NAMESPACE_COPY_DEST: &str = "itest-copy-dest";
const NAMESPACE_HEALTH_META: &str = "itest-health-meta";
const NAMESPACE_10K: &str = "itest-10k";
const NAMESPACE_WAL_COMPACT: &str = "itest-wal-compact";
const NAMESPACE_UPSERT_COND: &str = "itest-upsert-cond";
const NAMESPACE_ORDER_BY: &str = "itest-order-by";
const STRESS_DOCS: usize = 10_000;
const STRESS_BATCH: usize = 2_000;
const STRESS_DIM: usize = 128;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

fn openpuffer_bin() -> PathBuf {
    std::env::var_os("CARGO_BIN_EXE_openpuffer")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/debug/openpuffer")
        })
}

fn ns_prefix() -> String {
    format!("{ROOT_PREFIX}{NAMESPACE}/")
}

async fn s3_client(endpoint: &str) -> Client {
    let creds = Credentials::new(MINIO_USER, MINIO_PASSWORD, None, None, "integration-test");
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

async fn ensure_bucket(client: &Client, bucket: &str) {
    let _ = client.create_bucket().bucket(bucket).send().await;
}

async fn s3_object_exists(client: &Client, bucket: &str, key: &str) -> bool {
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

async fn list_keys_with_prefix(client: &Client, bucket: &str, prefix: &str) -> Vec<String> {
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

async fn assert_wal_layout_after_write(client: &Client, bucket: &str) {
    let meta = meta_key(NAMESPACE);
    assert!(
        s3_object_exists(client, bucket, &meta).await,
        "expected {meta} after upsert"
    );
    let wal = format!("{ROOT_PREFIX}{NAMESPACE}/wal/00000001.bin");
    assert!(
        s3_object_exists(client, bucket, &wal).await,
        "expected {wal} after upsert"
    );

    let docs_prefix = format!("{ROOT_PREFIX}{NAMESPACE}/docs/");
    let legacy_keys: Vec<_> = list_keys_with_prefix(client, bucket, &docs_prefix)
        .await
        .into_iter()
        .filter(|k| k.ends_with(".json"))
        .collect();
    assert!(
        legacy_keys.is_empty(),
        "legacy docs/*.json must not exist, found {legacy_keys:?}"
    );
    let manifest = format!("{ROOT_PREFIX}{NAMESPACE}/manifest.json");
    assert!(
        !s3_object_exists(client, bucket, &manifest).await,
        "legacy manifest.json must not exist"
    );
}

async fn assert_index_objects(client: &Client, bucket: &str) {
    let index_prefix = format!("{ROOT_PREFIX}{NAMESPACE}/index/");
    let keys = list_keys_with_prefix(client, bucket, &index_prefix).await;
    let has_fts = keys.iter().any(|k| k.contains("/index/fts-") && k.ends_with(".bin"));
    let has_centroids = keys
        .iter()
        .any(|k| k.ends_with("/index/centroids-l0.bin"));
    let has_filter = keys
        .iter()
        .any(|k| k.contains("/index/filter-") && k.ends_with(".bin"));
    assert!(has_fts, "expected index/fts-*.bin, keys={keys:?}");
    assert!(has_centroids, "expected index/centroids-l0.bin, keys={keys:?}");
    assert!(has_filter, "expected index/filter-*.bin, keys={keys:?}");
}

async fn wait_until_indexed(base_url: &str, timeout: Duration) {
    let client = reqwest::Client::new();
    let url = format!("{base_url}/v1/namespaces/{NAMESPACE}");
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("index_cursor never caught up to wal_commit_seq within {timeout:?}");
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

struct ServeHandle {
    child: Child,
    base_url: String,
}

impl ServeHandle {
    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn spawn(endpoint: &str, listen: &str) -> Self {
        Self::spawn_with_cache(endpoint, listen, None)
    }

    fn spawn_with_cache(endpoint: &str, listen: &str, cache_dir: Option<PathBuf>) -> Self {
        Self::spawn_with_options(endpoint, listen, cache_dir, None, None)
    }

    fn spawn_with_options(
        endpoint: &str,
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
            endpoint.to_string(),
            "--s3-bucket".to_string(),
            BUCKET.to_string(),
            "--s3-region".to_string(),
            "us-east-1".to_string(),
            "--s3-access-key".to_string(),
            MINIO_USER.to_string(),
            "--s3-secret-key".to_string(),
            MINIO_PASSWORD.to_string(),
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

    async fn wait_ready(&self) {
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

async fn fetch_namespace_meta(client: &Client, bucket: &str, namespace: &str) -> NamespaceMeta {
    let key = meta_key(namespace);
    let out = client
        .get_object()
        .bucket(bucket)
        .key(&key)
        .send()
        .await
        .expect("get meta.json");
    let bytes = out
        .body
        .collect()
        .await
        .expect("read meta body")
        .into_bytes();
    serde_json::from_slice(&bytes).expect("parse NamespaceMeta")
}

async fn upsert_batch(base_url: &str, namespace: &str, rows: Value) {
    write_batch(base_url, namespace, json!({ "upsert_rows": rows })).await;
}

async fn write_batch(base_url: &str, namespace: &str, body: Value) {
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

async fn wait_until_indexed_ns(base_url: &str, namespace: &str, timeout: Duration) {
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

async fn upsert_documents(base_url: &str) {
    let body = json!({
        "upsert_rows": [
            {
                "id": "doc-a",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "alpha bravo unique",
                    "tier": "pro"
                }
            },
            {
                "id": "doc-b",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "charlie delta",
                    "tier": "free"
                }
            },
            {
                "id": "doc-c",
                "attributes": {
                    "embedding": [0.9, 0.1, 0.0],
                    "text": "alpha charlie",
                    "tier": "pro"
                }
            }
        ]
    });
    let resp = reqwest::Client::new()
        .post(format!("{base_url}/v2/namespaces/{NAMESPACE}"))
        .json(&body)
        .send()
        .await
        .expect("upsert request");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "upsert failed: {}",
        resp.text().await.unwrap_or_default()
    );
}

async fn query_response_ns(
    base_url: &str,
    namespace: &str,
    body: Value,
) -> Value {
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

async fn query_ids_ns(
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

async fn query_ids(base_url: &str, rank_by: Value, filters: Option<Value>) -> Vec<String> {
    query_ids_ns(base_url, NAMESPACE, rank_by, filters).await
}

async fn assert_search_results(base_url: &str) {
    let vector_ids = query_ids(
        base_url,
        json!(["vector", "ANN", "embedding", [1.0, 0.0, 0.0]]),
        None,
    )
    .await;
    assert_eq!(
        vector_ids.first().map(String::as_str),
        Some("doc-a"),
        "vector top-1 should be doc-a, got {vector_ids:?}"
    );

    let fts_ids = query_ids(base_url, json!(["BM25", "text", "alpha"]), None).await;
    assert!(
        fts_ids.contains(&"doc-a".to_string()) && fts_ids.contains(&"doc-c".to_string()),
        "FTS should return doc-a and doc-c, got {fts_ids:?}"
    );

    let hybrid_ids = query_ids(
        base_url,
        json!([
            "Sum",
            ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            ["BM25", "text", "alpha"]
        ]),
        None,
    )
    .await;
    assert!(
        hybrid_ids.first().map(String::as_str) == Some("doc-a")
            || hybrid_ids.first().map(String::as_str) == Some("doc-c"),
        "hybrid should rank alpha+vector docs first, got {hybrid_ids:?}"
    );
    assert!(
        hybrid_ids.contains(&"doc-a".to_string()),
        "hybrid results should include doc-a, got {hybrid_ids:?}"
    );

    let filter_ids = query_ids(
        base_url,
        json!(["vector", "ANN", "embedding", [1.0, 0.0, 0.0]]),
        Some(json!(["tier", "Eq", "pro"])),
    )
    .await;
    assert!(
        filter_ids.contains(&"doc-a".to_string()) && filter_ids.contains(&"doc-c".to_string()),
        "filter tier=pro should include doc-a and doc-c, got {filter_ids:?}"
    );
    assert!(
        !filter_ids.contains(&"doc-b".to_string()),
        "filter tier=pro must exclude doc-b, got {filter_ids:?}"
    );
}

#[tokio::test]
async fn minio_wal_index_layout_queries_and_restart_persistence() {
    let minio = MinIO::default().start().await.expect("start MinIO container");
    let host = minio.get_host().await.expect("minio host");
    let port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");

    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");

    let mut serve1 = ServeHandle::spawn(&endpoint, &listen);
    serve1.wait_ready().await;
    upsert_documents(&serve1.base_url).await;

    assert_wal_layout_after_write(&s3, BUCKET).await;
    wait_until_indexed(&serve1.base_url, Duration::from_secs(30)).await;
    assert_index_objects(&s3, BUCKET).await;

    assert_search_results(&serve1.base_url).await;

    // Prove data survives serve process restart with only S3 backing (WAL + index).
    serve1.stop();
    drop(serve1);
    sleep(Duration::from_millis(500)).await;

    let serve2 = ServeHandle::spawn(&endpoint, &listen);
    serve2.wait_ready().await;

    assert_wal_layout_after_write(&s3, BUCKET).await;
    wait_until_indexed(&serve2.base_url, Duration::from_secs(10)).await;
    assert_index_objects(&s3, BUCKET).await;
    assert_search_results(&serve2.base_url).await;

    // Namespace prefix must not contain legacy doc JSON paths.
    let all_keys = list_keys_with_prefix(&s3, BUCKET, &ns_prefix()).await;
    let legacy_doc_keys: Vec<_> = all_keys
        .iter()
        .filter(|k| k.contains("/docs/") && k.ends_with(".json"))
        .collect();
    assert!(
        legacy_doc_keys.is_empty(),
        "no docs/{{id}}.json keys under namespace, found {legacy_doc_keys:?}"
    );
}

/// Three separate WAL commits (group-commit gap) → indexer advances cursor 3× with chained segments.
#[tokio::test]
async fn incremental_index_three_wal_batches_without_regression() {
    let minio = MinIO::default().start().await.expect("start MinIO container");
    let host = minio.get_host().await.expect("minio host");
    let port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");

    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn(&endpoint, &listen);
    serve.wait_ready().await;

    // >1s between writes so each upsert becomes its own WAL segment (default group commit).
    upsert_batch(
        &serve.base_url,
        NAMESPACE_INCR,
        json!([{
            "id": "batch-1",
            "attributes": {
                "embedding": [1.0, 0.0, 0.0],
                "text": "first batch wal one",
                "tier": "a"
            }
        }]),
    )
    .await;
    sleep(Duration::from_millis(1500)).await;
    wait_until_indexed_ns(&serve.base_url, NAMESPACE_INCR, Duration::from_secs(30)).await;

    let meta1 = fetch_namespace_meta(&s3, BUCKET, NAMESPACE_INCR).await;
    assert_eq!(meta1.index_cursor, 1);
    assert_eq!(meta1.wal_commit_seq, 1);
    assert_eq!(meta1.fts_segment_ids, vec![1]);
    assert_eq!(meta1.filter_segment_ids, vec![1]);

    upsert_batch(
        &serve.base_url,
        NAMESPACE_INCR,
        json!([{
            "id": "batch-2",
            "attributes": {
                "embedding": [0.0, 1.0, 0.0],
                "text": "second batch wal two",
                "tier": "b"
            }
        }]),
    )
    .await;
    sleep(Duration::from_millis(1500)).await;
    wait_until_indexed_ns(&serve.base_url, NAMESPACE_INCR, Duration::from_secs(30)).await;

    let meta2 = fetch_namespace_meta(&s3, BUCKET, NAMESPACE_INCR).await;
    assert_eq!(meta2.index_cursor, 2);
    assert_eq!(meta2.wal_commit_seq, 2);
    assert_eq!(meta2.fts_segment_ids, vec![1, 2]);
    assert_eq!(meta2.filter_segment_ids, vec![1, 2]);

    upsert_batch(
        &serve.base_url,
        NAMESPACE_INCR,
        json!([{
            "id": "batch-3",
            "attributes": {
                "embedding": [0.0, 0.0, 1.0],
                "text": "third batch wal three",
                "tier": "c"
            }
        }]),
    )
    .await;
    sleep(Duration::from_millis(1500)).await;
    wait_until_indexed_ns(&serve.base_url, NAMESPACE_INCR, Duration::from_secs(30)).await;

    let meta3 = fetch_namespace_meta(&s3, BUCKET, NAMESPACE_INCR).await;
    assert_eq!(meta3.index_cursor, 3);
    assert_eq!(meta3.wal_commit_seq, 3);
    assert_eq!(meta3.fts_segment_ids, vec![1, 2, 3]);
    assert_eq!(meta3.filter_segment_ids, vec![1, 2, 3]);
    assert_eq!(meta3.vector_segment_ids, vec![1, 2, 3]);

    let index_prefix = format!("{ROOT_PREFIX}{NAMESPACE_INCR}/index/");
    let keys = list_keys_with_prefix(&s3, BUCKET, &index_prefix).await;
    for seq in 1..=3 {
        let fts = format!("{ROOT_PREFIX}{NAMESPACE_INCR}/index/fts-{seq:08}.bin");
        let filter = format!("{ROOT_PREFIX}{NAMESPACE_INCR}/index/filter-{seq:08}.bin");
        assert!(keys.contains(&fts), "expected incremental fts segment {fts}");
        assert!(
            keys.contains(&filter),
            "expected incremental filter segment {filter}"
        );
    }

    let fts_ids = query_ids_ns(
        &serve.base_url,
        NAMESPACE_INCR,
        json!(["BM25", "text", "third"]),
        None,
    )
    .await;
    assert!(
        fts_ids.contains(&"batch-3".to_string()),
        "FTS should see batch-3 after incremental merges, got {fts_ids:?}"
    );
}

/// Warm endpoint populates disk cache; second query after reset uses HEAD-only (no S3 GetObject).
#[tokio::test]
async fn warm_cache_then_query_zero_s3_gets() {
    let minio = MinIO::default().start().await.expect("start MinIO container");
    let host = minio.get_host().await.expect("minio host");
    let port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");

    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let cache_dir = tempfile::tempdir().expect("cache tempdir");
    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn_with_cache(
        &endpoint,
        &listen,
        Some(cache_dir.path().to_path_buf()),
    );
    serve.wait_ready().await;

    upsert_batch(
        &serve.base_url,
        NAMESPACE_WARM,
        json!([{
            "id": "warm-doc",
            "attributes": {
                "embedding": [1.0, 0.0, 0.0],
                "text": "warm cache integration test",
                "tier": "pro"
            }
        }]),
    )
    .await;
    wait_until_indexed_ns(&serve.base_url, NAMESPACE_WARM, Duration::from_secs(30)).await;

    let client = reqwest::Client::new();
    let warm_resp = client
        .post(format!(
            "{}/v1/namespaces/{NAMESPACE_WARM}/warm",
            serve.base_url
        ))
        .send()
        .await
        .expect("warm request");
    assert_eq!(warm_resp.status(), StatusCode::OK, "warm failed");
    let warm_body: Value = warm_resp.json().await.expect("warm json");
    assert_eq!(warm_body["status"], "ok");
    assert!(warm_body["pinned"].as_bool().unwrap_or(false));
    assert!(warm_body["duration_ms"].as_u64().is_some());

    let reset = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");
    assert_eq!(reset.status(), StatusCode::OK);

    let _ = query_ids_ns(
        &serve.base_url,
        NAMESPACE_WARM,
        json!(["BM25", "text", "warm"]),
        None,
    )
    .await;

    let stats = client
        .get(format!("{}/v1/debug/cache-stats", serve.base_url))
        .send()
        .await
        .expect("cache stats");
    let stats_body: Value = stats.json().await.expect("stats json");
    assert_eq!(
        stats_body["s3_get_count"].as_u64(),
        Some(0),
        "query after warm should not S3 GetObject index segments (disk cache hit)"
    );
}

/// Schema on write persists in meta; delete_by_filter removes matching docs from queries.
#[tokio::test]
async fn schema_on_write_and_delete_by_filter() {
    let minio = MinIO::default().start().await.expect("start MinIO container");
    let host = minio.get_host().await.expect("minio host");
    let port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");

    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn(&endpoint, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_DEL_FILTER,
        json!({
            "schema": {
                "text": {"type": "string", "full_text_search": true},
                "tier": {"type": "string", "filterable": true},
                "embedding": "[3]f32"
            },
            "upsert_rows": [
                {
                    "id": "doc-a",
                    "attributes": {
                        "embedding": [1.0, 0.0, 0.0],
                        "text": "alpha bravo",
                        "tier": "pro"
                    }
                },
                {
                    "id": "doc-b",
                    "attributes": {
                        "embedding": [0.0, 1.0, 0.0],
                        "text": "charlie delta",
                        "tier": "free"
                    }
                }
            ]
        }),
    )
    .await;
    wait_until_indexed_ns(&serve.base_url, NAMESPACE_DEL_FILTER, Duration::from_secs(30)).await;

    let meta = fetch_namespace_meta(&s3, BUCKET, NAMESPACE_DEL_FILTER).await;
    assert_eq!(
        meta.schema["text"]["full_text_search"],
        json!(true)
    );
    assert_eq!(meta.schema["tier"]["filterable"], json!(true));

    write_batch(
        &serve.base_url,
        NAMESPACE_DEL_FILTER,
        json!({ "delete_by_filter": ["tier", "Eq", "free"] }),
    )
    .await;
    sleep(Duration::from_millis(1500)).await;

    let all_ids = query_ids_ns(
        &serve.base_url,
        NAMESPACE_DEL_FILTER,
        json!(["BM25", "text", "alpha"]),
        None,
    )
    .await;
    assert!(
        all_ids.contains(&"doc-a".to_string()),
        "doc-a should remain after delete_by_filter, got {all_ids:?}"
    );
    assert!(
        !all_ids.contains(&"doc-b".to_string()),
        "doc-b (tier=free) must be removed by delete_by_filter, got {all_ids:?}"
    );

    let filter_ids = query_ids_ns(
        &serve.base_url,
        NAMESPACE_DEL_FILTER,
        json!(["BM25", "text", "charlie"]),
        Some(json!(["tier", "Eq", "free"])),
    )
    .await;
    assert!(
        filter_ids.is_empty(),
        "deleted free-tier doc must not match filter query, got {filter_ids:?}"
    );
}

/// patch_rows merges attributes in WAL; patched text is visible in FTS after indexing.
#[tokio::test]
async fn patch_rows_updates_fts_after_index() {
    let minio = MinIO::default().start().await.expect("start MinIO container");
    let host = minio.get_host().await.expect("minio host");
    let port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");

    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn(&endpoint, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_PATCH,
        json!({
            "schema": {
                "text": {"type": "string", "full_text_search": true},
                "embedding": "[3]f32"
            },
            "upsert_rows": [{
                "id": "doc-patch",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "original keyword"
                }
            }]
        }),
    )
    .await;
    wait_until_indexed_ns(&serve.base_url, NAMESPACE_PATCH, Duration::from_secs(30)).await;

    let before = query_ids_ns(
        &serve.base_url,
        NAMESPACE_PATCH,
        json!(["BM25", "text", "original"]),
        None,
    )
    .await;
    assert!(
        before.contains(&"doc-patch".to_string()),
        "FTS should find doc before patch, got {before:?}"
    );

    write_batch(
        &serve.base_url,
        NAMESPACE_PATCH,
        json!({
            "patch_rows": [{
                "id": "doc-patch",
                "attributes": { "text": "patched keyword unique" }
            }]
        }),
    )
    .await;
    wait_until_indexed_ns(&serve.base_url, NAMESPACE_PATCH, Duration::from_secs(30)).await;

    let after = query_ids_ns(
        &serve.base_url,
        NAMESPACE_PATCH,
        json!(["BM25", "text", "patched"]),
        None,
    )
    .await;
    assert!(
        after.contains(&"doc-patch".to_string()),
        "FTS should find doc after patch, got {after:?}"
    );

    let stale = query_ids_ns(
        &serve.base_url,
        NAMESPACE_PATCH,
        json!(["BM25", "text", "original"]),
        None,
    )
    .await;
    assert!(
        !stale.contains(&"doc-patch".to_string()),
        "old token should not match after text patch, got {stale:?}"
    );

    // Patches to missing ids are ignored (turbopuffer semantics).
    write_batch(
        &serve.base_url,
        NAMESPACE_PATCH,
        json!({
            "patch_rows": [{
                "id": "no-such-doc",
                "attributes": { "text": "ghost" }
            }]
        }),
    )
    .await;

    // Vector fields cannot be patched.
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/v2/namespaces/{}",
            serve.base_url, NAMESPACE_PATCH
        ))
        .json(&json!({
            "patch_rows": [{
                "id": "doc-patch",
                "attributes": { "embedding": [0.0, 1.0, 0.0] }
            }]
        }))
        .send()
        .await
        .expect("patch vector request");
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "patching vector field must return 400: {}",
        resp.text().await.unwrap_or_default()
    );
}

/// Five rapid writes with batch_ops=1 cannot exceed two WAL files in the first 1.5s.
#[tokio::test]
async fn wal_commit_rate_max_one_per_second() {
    let container = MinIO::default().start().await.expect("start minio");
    let host = container.get_host().await.expect("minio host");
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");
    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn_with_options(&endpoint, &listen, None, Some(1), None);
    serve.wait_ready().await;

    let http = reqwest::Client::new();
    let write_url = format!("{}/v2/namespaces/{NAMESPACE_WAL_RATE}", serve.base_url);

    let started = std::time::Instant::now();
    let mut tasks = Vec::new();
    for i in 0..5 {
        let client = http.clone();
        let url = write_url.clone();
        tasks.push(tokio::spawn(async move {
            let resp = client
                .post(&url)
                .json(&json!({
                    "upsert_rows": [{
                        "id": format!("rate-{i}"),
                        "attributes": { "text": format!("wal rate test {i}") }
                    }]
                }))
                .send()
                .await
                .expect("rate-limit write request");
            (i, resp.status())
        }));
    }
    for task in tasks {
        let (i, status) = task.await.expect("join rate-limit write");
        assert_eq!(status, StatusCode::OK, "write rate-{i} failed");
    }

    assert!(
        started.elapsed() < Duration::from_millis(1600),
        "five rate-limited writes should not all block past 1.6s"
    );

    let wal_prefix = format!("{ROOT_PREFIX}{NAMESPACE_WAL_RATE}/wal/");
    let wal_keys = list_keys_with_prefix(&s3, BUCKET, &wal_prefix).await;
    let mut seqs: Vec<u64> = wal_keys
        .iter()
        .filter_map(|k| {
            let name = k.rsplit('/').next()?;
            name.strip_suffix(".bin")?.parse().ok()
        })
        .collect();
    seqs.sort_unstable();
    assert!(
        seqs.len() <= 2,
        "at most 2 WAL commits in first ~1.5s, got {seqs:?} keys={wal_keys:?}"
    );
    if seqs.len() == 2 {
        assert_eq!(seqs[1], seqs[0] + 1, "wal seq gap: {seqs:?}");
    }

    let meta = fetch_namespace_meta(&s3, BUCKET, NAMESPACE_WAL_RATE).await;
    assert!(
        meta.wal_commit_seq >= 1 && meta.wal_commit_seq <= 2,
        "five batched writes → 1–2 WAL commits, not one per row: meta={meta:?}"
    );
    assert_eq!(
        seqs.len(),
        meta.wal_commit_seq as usize,
        "wal file count must match commit seq"
    );
    assert_eq!(seqs.last().copied(), Some(meta.wal_commit_seq));

    use openpuffer::namespace::replay_wal_range;
    use std::collections::HashMap;

    let mut docs = HashMap::new();
    replay_wal_range(
        &s3,
        BUCKET,
        NAMESPACE_WAL_RATE,
        &mut docs,
        1,
        meta.wal_commit_seq,
    )
    .await
    .expect("replay wal");
    for i in 0..5 {
        assert!(
            docs.contains_key(&format!("rate-{i}")),
            "missing rate-{i}, docs={docs:?}"
        );
    }
}

/// Ten parallel HTTP clients upsert distinct doc ids; WAL seq monotonic, no lost docs.
#[tokio::test]
async fn concurrent_writes_ten_parallel_clients() {
    let container = MinIO::default().start().await.expect("start minio");
    let host = container.get_host().await.expect("minio host");
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");
    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn(&endpoint, &listen);
    serve.wait_ready().await;

    let http = reqwest::Client::new();
    let write_url = format!("{}/v2/namespaces/{NAMESPACE_CONCURRENT}", serve.base_url);

    let mut tasks = Vec::new();
    for i in 0..10 {
        let client = http.clone();
        let url = write_url.clone();
        tasks.push(tokio::spawn(async move {
            let resp = client
                .post(&url)
                .json(&json!({
                    "upsert_rows": [{
                        "id": format!("doc-{i}"),
                        "attributes": { "text": format!("concurrent write {i}") }
                    }]
                }))
                .send()
                .await
                .expect("concurrent write request");
            let status = resp.status();
            let body: Value = resp.json().await.expect("write response json");
            (i, status, body)
        }));
    }

    let mut doc_ids = Vec::new();
    for task in tasks {
        let (i, status, body) = task.await.expect("join concurrent write");
        assert_eq!(status, StatusCode::OK, "write doc-{i} failed: {body}");
        assert_eq!(body["rows_affected"].as_u64(), Some(1), "doc-{i}: {body}");
        assert_eq!(body["rows_upserted"].as_u64(), Some(1));
        doc_ids.push(format!("doc-{i}"));
    }

    // Allow group-commit timer (default 1s) to flush any buffered writes.
    sleep(Duration::from_millis(1200)).await;

    let meta = fetch_namespace_meta(&s3, BUCKET, NAMESPACE_CONCURRENT).await;
    assert!(
        meta.wal_commit_seq >= 1 && meta.wal_commit_seq <= 10,
        "wal_commit_seq must advance monotonically (1..=10 commits), meta={meta:?}"
    );

    let wal_prefix = format!("{ROOT_PREFIX}{NAMESPACE_CONCURRENT}/wal/");
    let wal_keys = list_keys_with_prefix(&s3, BUCKET, &wal_prefix).await;
    let mut seqs: Vec<u64> = wal_keys
        .iter()
        .filter_map(|k| {
            let name = k.rsplit('/').next()?;
            name.strip_suffix(".bin")?.parse().ok()
        })
        .collect();
    seqs.sort_unstable();
    assert_eq!(seqs.first(), Some(&1), "wal keys: {wal_keys:?}");
    assert_eq!(
        seqs.last(),
        Some(&meta.wal_commit_seq),
        "wal seqs must end at commit point, got {seqs:?}"
    );
    for w in seqs.windows(2) {
        assert_eq!(w[1], w[0] + 1, "wal seq gap: {seqs:?}");
    }

    use openpuffer::namespace::replay_wal_range;
    use std::collections::HashMap;

    let mut docs = HashMap::new();
    replay_wal_range(
        &s3,
        BUCKET,
        NAMESPACE_CONCURRENT,
        &mut docs,
        1,
        meta.wal_commit_seq,
    )
    .await
    .expect("replay wal");
    assert_eq!(
        docs.len(),
        10,
        "lost docs in WAL replay (expected doc-0..doc-9): {:?}",
        docs.keys().collect::<Vec<_>>()
    );
    for id in doc_ids {
        assert!(docs.contains_key(&id), "missing doc {id}");
    }
}

/// Regression: first request after restart must not be a write that drops prior WAL replay.
#[tokio::test]
async fn write_after_restart_before_query_preserves_prior_docs() {
    let minio = MinIO::default().start().await.expect("start MinIO container");
    let host = minio.get_host().await.expect("minio host");
    let port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");

    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");

    let mut serve1 = ServeHandle::spawn(&endpoint, &listen);
    serve1.wait_ready().await;
    upsert_batch(
        &serve1.base_url,
        NAMESPACE_RESTART_WRITE,
        json!([{
            "id": "seed-a",
            "attributes": {
                "embedding": [1.0, 0.0, 0.0],
                "text": "seed alpha",
                "tier": "seed"
            }
        }]),
    )
    .await;
    wait_until_indexed_ns(&serve1.base_url, NAMESPACE_RESTART_WRITE, Duration::from_secs(30))
        .await;

    serve1.stop();
    drop(serve1);
    sleep(Duration::from_millis(500)).await;

    let serve2 = ServeHandle::spawn(&endpoint, &listen);
    serve2.wait_ready().await;

    // Write before any query — previously created an empty in-memory view and lost WAL history.
    write_batch(
        &serve2.base_url,
        NAMESPACE_RESTART_WRITE,
        json!({
            "upsert_rows": [{
                "id": "post-restart",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "after restart",
                    "tier": "new"
                }
            }]
        }),
    )
    .await;

    let ids = query_ids_ns(
        &serve2.base_url,
        NAMESPACE_RESTART_WRITE,
        json!(["BM25", "text", "seed"]),
        None,
    )
    .await;
    assert!(
        ids.contains(&"seed-a".to_string()),
        "prior WAL doc must survive cold-cache write, got {ids:?}"
    );
}

async fn export_all_ids(base_url: &str, namespace: &str, page_limit: Option<usize>) -> Vec<String> {
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

/// Export reconstructs all document ids from WAL snapshot (paginated `last_id`).
#[tokio::test]
async fn export_after_writes_returns_all_doc_ids() {
    let minio = MinIO::default().start().await.expect("start MinIO container");
    let host = minio.get_host().await.expect("minio host");
    let port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");

    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn(&endpoint, &listen);
    serve.wait_ready().await;

    let expected = ["exp-a", "exp-b", "exp-c"];
    upsert_batch(
        &serve.base_url,
        NAMESPACE_EXPORT,
        json!([
            {"id": "exp-a", "attributes": {"text": "one", "tier": "x"}},
            {"id": "exp-b", "attributes": {"text": "two", "tier": "x"}},
            {"id": "exp-c", "attributes": {"text": "three", "tier": "x"}},
        ]),
    )
    .await;

    let ids_full = export_all_ids(&serve.base_url, NAMESPACE_EXPORT, None).await;
    for id in &expected {
        assert!(ids_full.contains(&id.to_string()), "export missing {id}, got {ids_full:?}");
    }
    assert_eq!(ids_full.len(), expected.len());

    // Paginate with limit=1 — three pages, same ids.
    let ids_paged = export_all_ids(&serve.base_url, NAMESPACE_EXPORT, Some(1)).await;
    assert_eq!(ids_paged, ids_full);

    // POST export with ndjson body
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/v1/namespaces/{NAMESPACE_EXPORT}/export",
            serve.base_url
        ))
        .json(&json!({"format": "ndjson"}))
        .send()
        .await
        .expect("export POST");
    assert_eq!(resp.status(), StatusCode::OK);
    let commit_seq = resp
        .headers()
        .get("x-openpuffer-wal-commit-seq")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(commit_seq > 0);
    let body = resp.text().await.expect("ndjson body");
    let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), expected.len());
    for line in lines {
        let row: Value = serde_json::from_str(line).expect("ndjson row");
        assert!(row["id"].is_string());
    }

    let meta = fetch_namespace_meta(&s3, BUCKET, NAMESPACE_EXPORT).await;
    assert_eq!(meta.wal_commit_seq, commit_seq);
}

/// `copy_from_namespace` clones S3 layout; query on destination returns same documents.
#[tokio::test]
async fn copy_from_namespace_returns_same_docs_on_dest() {
    let minio = MinIO::default().start().await.expect("start MinIO container");
    let host = minio.get_host().await.expect("minio host");
    let port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");

    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn(&endpoint, &listen);
    serve.wait_ready().await;

    upsert_batch(
        &serve.base_url,
        NAMESPACE_COPY_SRC,
        json!([
            {"id": "copy-a", "attributes": {"text": "alpha copy", "embedding": [1.0, 0.0, 0.0]}},
            {"id": "copy-b", "attributes": {"text": "beta copy", "embedding": [0.0, 1.0, 0.0]}},
        ]),
    )
    .await;
    wait_until_indexed_ns(&serve.base_url, NAMESPACE_COPY_SRC, Duration::from_secs(90)).await;

    write_batch(
        &serve.base_url,
        NAMESPACE_COPY_DEST,
        json!({"copy_from_namespace": NAMESPACE_COPY_SRC}),
    )
    .await;

    let src_meta = fetch_namespace_meta(&s3, BUCKET, NAMESPACE_COPY_SRC).await;
    let dest_meta = fetch_namespace_meta(&s3, BUCKET, NAMESPACE_COPY_DEST).await;
    assert_eq!(
        dest_meta.wal_commit_seq, src_meta.wal_commit_seq,
        "dest should inherit WAL commit seq from source"
    );

    let dest_prefix = format!("{ROOT_PREFIX}{NAMESPACE_COPY_DEST}/");
    let dest_keys = list_keys_with_prefix(&s3, BUCKET, &dest_prefix).await;
    assert!(
        dest_keys.iter().any(|k| k.contains("/wal/")),
        "dest missing wal objects: {dest_keys:?}"
    );
    assert!(
        dest_keys.iter().any(|k| k.ends_with("meta.json")),
        "dest missing meta.json: {dest_keys:?}"
    );

    let fts_ids = query_ids_ns(
        &serve.base_url,
        NAMESPACE_COPY_DEST,
        json!(["BM25", "text", "alpha"]),
        None,
    )
    .await;
    assert!(
        fts_ids.iter().any(|id| id == "copy-a"),
        "dest FTS query should find copy-a, got {fts_ids:?}"
    );

    let vector_ids = query_ids_ns(
        &serve.base_url,
        NAMESPACE_COPY_DEST,
        json!(["vector", "ANN", "embedding", [1.0, 0.0, 0.0]]),
        None,
    )
    .await;
    assert_eq!(
        vector_ids.first().map(String::as_str),
        Some("copy-a"),
        "dest vector top-1 should be copy-a, got {vector_ids:?}"
    );
}

#[tokio::test]
async fn deep_health_and_namespace_metadata_fields() {
    let container = MinIO::default().start().await.expect("start minio");
    let host = container.get_host().await.expect("minio host");
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");
    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let mut serve = ServeHandle::spawn(&endpoint, &listen);
    serve.wait_ready().await;

    let ns = NAMESPACE_HEALTH_META;
    upsert_batch(
        &serve.base_url,
        ns,
        json!([
            {
                "id": "doc-a",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "alpha bravo unique",
                    "tier": "pro"
                }
            },
            {
                "id": "doc-b",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "charlie delta",
                    "tier": "free"
                }
            },
            {
                "id": "doc-c",
                "attributes": {
                    "embedding": [0.9, 0.1, 0.0],
                    "text": "alpha charlie",
                    "tier": "pro"
                }
            }
        ]),
    )
    .await;
    wait_until_indexed_ns(&serve.base_url, ns, Duration::from_secs(90)).await;

    let client = reqwest::Client::new();
    let health = client
        .get(format!("{}/health?deep=1", serve.base_url))
        .send()
        .await
        .expect("deep health");
    assert_eq!(health.status(), StatusCode::OK);
    let hv: Value = health.json().await.expect("health json");
    assert_eq!(hv["status"].as_str(), Some("ok"));
    assert_eq!(hv["s3"].as_str(), Some("ok"));
    assert_eq!(hv["deep"].as_bool(), Some(true));

    let meta_resp = client
        .get(format!("{base_url}/v1/namespaces/{ns}", base_url = serve.base_url))
        .send()
        .await
        .expect("namespace metadata");
    assert_eq!(meta_resp.status(), StatusCode::OK);
    let meta: Value = meta_resp.json().await.expect("metadata json");
    assert_eq!(meta["approx_row_count"].as_u64(), Some(3));
    assert_eq!(meta["index_status"].as_str(), Some("up_to_date"));
    assert!(meta["unindexed_bytes"].as_u64().is_some());
    assert_eq!(meta["index_cursor"].as_u64(), meta["wal_commit_seq"].as_u64());

    // Unindexed tail: write without waiting for indexer.
    upsert_batch(
        &serve.base_url,
        ns,
        json!([{
            "id": "doc-meta-lag",
            "attributes": {"text": "lag probe", "embedding": [0.1, 0.2, 0.0], "tier": "pro"}
        }]),
    )
    .await;
    let lag_meta = client
        .get(format!("{base_url}/v1/namespaces/{ns}", base_url = serve.base_url))
        .send()
        .await
        .expect("lag metadata")
        .json::<Value>()
        .await
        .expect("lag json");
    assert_eq!(lag_meta["index_status"].as_str(), Some("catching_up"));
    assert!(
        lag_meta["unindexed_bytes"].as_u64().unwrap_or(0) > 0,
        "expected unindexed_bytes > 0 while catching up"
    );
    assert_eq!(lag_meta["approx_row_count"].as_u64(), Some(4));

    serve.stop();
}

fn stress_upsert_columns(start: usize, count: usize) -> Value {
    let mut ids = Vec::with_capacity(count);
    let mut texts = Vec::with_capacity(count);
    let mut embeddings = Vec::with_capacity(count);
    for i in start..start + count {
        ids.push(json!(format!("doc-{i}")));
        texts.push(json!(format!("stressterm document number {i}")));
        let emb: Vec<f64> = (0..STRESS_DIM)
            .map(|d| ((i * STRESS_DIM + d) as f64 * 0.001).sin())
            .collect();
        embeddings.push(json!(emb));
    }
    json!({
        "id": ids,
        "text": texts,
        "embedding": embeddings
    })
}

/// 10k-column upsert, background index, warm, ANN + FTS under candidate-ratio guard.
#[tokio::test]
async fn ten_thousand_docs_indexed_query() {
    let test_started = std::time::Instant::now();
    let minio = MinIO::default().start().await.expect("start MinIO container");
    let host = minio.get_host().await.expect("minio host");
    let port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");

    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let cache_dir = tempfile::tempdir().expect("cache tempdir");
    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn_with_options(
        &endpoint,
        &listen,
        Some(cache_dir.path().to_path_buf()),
        Some(10_000),
        None,
    );
    serve.wait_ready().await;

    let ns = NAMESPACE_10K;
    let schema = json!({
        "text": {"type": "string", "full_text_search": true},
        "embedding": "[128]f32"
    });

    let batches = STRESS_DOCS / STRESS_BATCH;
    assert_eq!(batches * STRESS_BATCH, STRESS_DOCS);

    let write_started = std::time::Instant::now();
    for b in 0..batches {
        if b > 0 {
            sleep(Duration::from_millis(1100)).await;
        }
        let start = b * STRESS_BATCH;
        let mut body = json!({ "upsert_columns": stress_upsert_columns(start, STRESS_BATCH) });
        if b == 0 {
            body["schema"] = schema.clone();
        }
        write_batch(&serve.base_url, ns, body).await;
    }
    sleep(Duration::from_millis(1200)).await;
    let write_elapsed = write_started.elapsed();

    wait_until_indexed_ns(&serve.base_url, ns, Duration::from_secs(120)).await;
    let index_elapsed = write_started.elapsed();

    let warm_resp = reqwest::Client::new()
        .post(format!("{}/v1/namespaces/{ns}/warm", serve.base_url))
        .send()
        .await
        .expect("warm request");
    assert_eq!(warm_resp.status(), StatusCode::OK);

    let query_vec: Vec<f64> = (0..STRESS_DIM)
        .map(|d| (d as f64 * 0.02).cos())
        .collect();
    let vector_body = json!({
        "rank_by": ["vector", "ANN", "embedding", query_vec],
        "top_k": 10
    });
    let vector_resp = query_response_ns(&serve.base_url, ns, vector_body).await;
    let rows = vector_resp["rows"].as_array().expect("vector rows");
    assert!(!rows.is_empty(), "vector query returned no rows");
    assert!(rows.len() <= 10, "top_k=10 but got {} rows", rows.len());
    let perf = vector_resp["performance"].as_object().expect("performance");
    let ratio = perf["candidates_ratio"].as_f64().expect("candidates_ratio");
    assert!(
        ratio < 0.15,
        "candidates_ratio {ratio} must be < 0.15 for 10k indexed ANN"
    );
    assert_eq!(perf["approx_namespace_size"].as_u64(), Some(STRESS_DOCS as u64));

    let fts_resp = query_response_ns(
        &serve.base_url,
        ns,
        json!({
            "rank_by": ["BM25", "text", "stressterm"],
            "top_k": 10
        }),
    )
    .await;
    let fts_rows = fts_resp["rows"].as_array().expect("fts rows");
    assert!(
        !fts_rows.is_empty(),
        "FTS on common term stressterm should return hits"
    );
    assert!(
        fts_rows.len() <= 10,
        "FTS top_k=10 but got {} rows",
        fts_rows.len()
    );

    let meta = fetch_namespace_meta(&s3, BUCKET, ns).await;
    assert_eq!(meta.index_cursor, meta.wal_commit_seq);
    assert!(meta.wal_commit_seq >= 1);

    eprintln!(
        "ten_thousand_docs_indexed_query: writes={write_elapsed:?} index+query={:?} wal_commits={}",
        index_elapsed,
        meta.wal_commit_seq
    );
    assert!(
        test_started.elapsed() < Duration::from_secs(120),
        "test exceeded 120s wall clock"
    );
}

/// Fifteen WAL commits → indexer catches up → compaction deletes indexed segments; cold query still works.
#[tokio::test]
async fn wal_compaction_after_full_index_query_still_works() {
    let minio = MinIO::default().start().await.expect("start MinIO container");
    let host = minio.get_host().await.expect("minio host");
    let port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");

    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let mut serve = ServeHandle::spawn_with_options(
        &endpoint,
        &listen,
        None,
        Some(1),
        Some(50),
    );
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_WAL_COMPACT,
        json!({
            "schema": {
                "text": { "type": "string", "full_text_search": true },
                "embedding": "[3]f32"
            },
            "upsert_rows": [{
                "id": "compact-0",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "wal compact unique zero"
                }
            }]
        }),
    )
    .await;

    for i in 1..15 {
        upsert_batch(
            &serve.base_url,
            NAMESPACE_WAL_COMPACT,
            json!([{
                "id": format!("compact-{i}"),
                "attributes": {
                    "embedding": [0.1 * i as f64, 0.2, 0.3],
                    "text": format!("wal compact unique term {i}")
                }
            }]),
        )
        .await;
    }

    wait_until_indexed_ns(
        &serve.base_url,
        NAMESPACE_WAL_COMPACT,
        Duration::from_secs(90),
    )
    .await;

    let snapshot_key = format!("{ROOT_PREFIX}{NAMESPACE_WAL_COMPACT}/wal/snapshot.bin");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut meta = fetch_namespace_meta(&s3, BUCKET, NAMESPACE_WAL_COMPACT).await;
    loop {
        if meta.wal_snapshot_seq > 0 && s3_object_exists(&s3, BUCKET, &snapshot_key).await {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            let wal_prefix = format!("{ROOT_PREFIX}{NAMESPACE_WAL_COMPACT}/wal/");
            let wal_keys = list_keys_with_prefix(&s3, BUCKET, &wal_prefix).await;
            let mut missing = Vec::new();
            for seq in 1..=meta.wal_commit_seq {
                let key = format!("{wal_prefix}{seq:08}.bin");
                if !s3_object_exists(&s3, BUCKET, &key).await {
                    missing.push(seq);
                }
            }
            panic!(
                "wal compaction did not finish within 30s, meta={meta:?} wal_keys={wal_keys:?} missing_seqs={missing:?}"
            );
        }
        sleep(Duration::from_millis(250)).await;
        meta = fetch_namespace_meta(&s3, BUCKET, NAMESPACE_WAL_COMPACT).await;
    }

    assert!(
        meta.wal_commit_seq >= 15,
        "expected >=15 wal commits, meta={meta:?}"
    );
    assert_eq!(meta.index_cursor, meta.wal_commit_seq);
    assert!(
        meta.wal_snapshot_seq > 0,
        "wal_snapshot_seq should be set after compaction, meta={meta:?}"
    );

    let wal_prefix = format!("{ROOT_PREFIX}{NAMESPACE_WAL_COMPACT}/wal/");
    let wal_keys = list_keys_with_prefix(&s3, BUCKET, &wal_prefix).await;
    assert!(
        wal_keys.iter().any(|k| k == &snapshot_key),
        "expected wal/snapshot.bin, keys={wal_keys:?}"
    );

    let first_wal = format!("{wal_prefix}00000001.bin");
    assert!(
        !s3_object_exists(&s3, BUCKET, &first_wal).await,
        "indexed wal segment 00000001.bin should be deleted after compaction"
    );

    let segment_wals: Vec<_> = wal_keys
        .iter()
        .filter(|k| {
            k.starts_with(&wal_prefix)
                && k.ends_with(".bin")
                && !k.ends_with("snapshot.bin")
        })
        .collect();
    assert!(
        segment_wals.len() <= 3,
        "expected at most 3 retained wal segments, got {segment_wals:?}"
    );

    serve.stop();
    // Cold batched load path (`--cache-dir=""`) must not refetch deleted WAL segments.
    let serve2 = ServeHandle::spawn_with_options(
        &endpoint,
        &listen,
        Some(PathBuf::from("")),
        Some(1),
        Some(50),
    );
    serve2.wait_ready().await;

    let fts_ids = query_ids_ns(
        &serve2.base_url,
        NAMESPACE_WAL_COMPACT,
        json!(["BM25", "text", "compact unique"]),
        None,
    )
    .await;
    assert!(
        fts_ids.iter().any(|id| id.starts_with("compact-")),
        "FTS after wal compaction + cold restart, ids={fts_ids:?}"
    );

    let vector_ids = query_ids_ns(
        &serve2.base_url,
        NAMESPACE_WAL_COMPACT,
        json!(["vector", "ANN", "embedding", [1.0, 0.0, 0.0]]),
        None,
    )
    .await;
    assert!(
        vector_ids.contains(&"compact-0".to_string()),
        "vector query after compaction, ids={vector_ids:?}"
    );
}

/// `upsert_condition` with `["id","Eq",null]`: insert new ids, skip overwrites.
#[tokio::test]
async fn upsert_condition_insert_if_not_exists() {
    let container = MinIO::default().start().await.expect("start minio");
    let host = container.get_host().await.expect("minio host");
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");
    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn(&endpoint, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_UPSERT_COND,
        json!({
            "upsert_rows": [{
                "id": "exists-1",
                "attributes": { "name": "original" }
            }]
        }),
    )
    .await;
    sleep(Duration::from_millis(1200)).await;

    let http = reqwest::Client::new();
    let write_url = format!(
        "{}/v2/namespaces/{NAMESPACE_UPSERT_COND}",
        serve.base_url
    );
    let resp = http
        .post(&write_url)
        .json(&json!({
            "upsert_condition": ["id", "Eq", null],
            "upsert_rows": [
                { "id": "exists-1", "attributes": { "name": "should-not-apply" } },
                { "id": "new-1", "attributes": { "name": "inserted" } }
            ]
        }))
        .send()
        .await
        .expect("conditional upsert");
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.expect("write json");
    assert_eq!(body["rows_upserted"].as_u64(), Some(1), "body={body}");
    assert_eq!(body["rows_affected"].as_u64(), Some(1));

    sleep(Duration::from_millis(1200)).await;

    let export = http
        .get(format!(
            "{}/v1/namespaces/{NAMESPACE_UPSERT_COND}/export",
            serve.base_url
        ))
        .send()
        .await
        .expect("export");
    assert_eq!(export.status(), StatusCode::OK);
    let exported: Value = export.json().await.expect("export json");
    let rows = exported["rows"].as_array().expect("export rows");
    let mut names: HashMap<String, String> = HashMap::new();
    for row in rows {
        let id = row["id"].as_str().expect("id");
        let name = row["attributes"]["name"]
            .as_str()
            .expect("name attr")
            .to_string();
        names.insert(id.to_string(), name);
    }
    assert_eq!(
        names.get("exists-1").map(String::as_str),
        Some("original"),
        "conditional upsert must not overwrite existing doc, names={names:?}"
    );
    assert_eq!(
        names.get("new-1").map(String::as_str),
        Some("inserted"),
        "new doc must be inserted, names={names:?}"
    );
}

/// `order_by` breaks ties after `rank_by` scoring (turbopuffer attribute sort shape).
#[tokio::test]
async fn order_by_sorts_tied_bm25_results_by_attribute() {
    let container = MinIO::default().start().await.expect("start minio");
    let host = container.get_host().await.expect("minio host");
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("minio api port");
    let endpoint = format!("http://{host}:{port}");
    let s3 = s3_client(&endpoint).await;
    ensure_bucket(&s3, BUCKET).await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn(&endpoint, &listen);
    serve.wait_ready().await;

    let shared_text = "orderby tie breaker shared tokens";
    write_batch(
        &serve.base_url,
        NAMESPACE_ORDER_BY,
        json!({
            "upsert_rows": [
                { "id": "ob-a", "attributes": { "text": shared_text, "seq": 10 } },
                { "id": "ob-b", "attributes": { "text": shared_text, "seq": 30 } },
                { "id": "ob-c", "attributes": { "text": shared_text, "seq": 20 } }
            ]
        }),
    )
    .await;
    sleep(Duration::from_millis(1200)).await;

    let v = query_response_ns(
        &serve.base_url,
        NAMESPACE_ORDER_BY,
        json!({
            "rank_by": ["BM25", "text", "orderby tie breaker"],
            "top_k": 3,
            "order_by": ["seq", "desc"]
        }),
    )
    .await;
    let ids: Vec<String> = v["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| r["id"].as_str().expect("id").to_string())
        .collect();
    assert_eq!(
        ids,
        vec!["ob-b".to_string(), "ob-c".to_string(), "ob-a".to_string()],
        "order_by seq desc should sort tied BM25 hits, got {ids:?}"
    );
}