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
        .any(|k| k.ends_with("/index/centroids.bin"));
    let has_filter = keys
        .iter()
        .any(|k| k.contains("/index/filter-") && k.ends_with(".bin"));
    assert!(has_fts, "expected index/fts-*.bin, keys={keys:?}");
    assert!(has_centroids, "expected index/centroids.bin, keys={keys:?}");
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
    let v: Value = resp.json().await.expect("query json");
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