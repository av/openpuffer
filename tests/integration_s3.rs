//! S3 round-trip integration tests against MinIO via testcontainers.
//!
//! Flow: MinIO container → `openpuffer serve` → upsert → vector / FTS / hybrid query
//! → kill serve → new serve (same S3) → queries still return data (restart persistence).

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
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
        let bin = openpuffer_bin();
        assert!(
            bin.exists(),
            "openpuffer binary not found at {}; run `cargo build` first",
            bin.display()
        );
        let child = Command::new(&bin)
            .args([
                "serve",
                "--listen",
                listen,
                "--s3-endpoint",
                endpoint,
                "--s3-bucket",
                BUCKET,
                "--s3-region",
                "us-east-1",
                "--s3-access-key",
                MINIO_USER,
                "--s3-secret-key",
                MINIO_PASSWORD,
            ])
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

async fn upsert_documents(base_url: &str) {
    let body = json!({
        "upsert_rows": [
            {
                "id": "doc-a",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "alpha bravo unique"
                }
            },
            {
                "id": "doc-b",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "charlie delta"
                }
            },
            {
                "id": "doc-c",
                "attributes": {
                    "embedding": [0.9, 0.1, 0.0],
                    "text": "alpha charlie"
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
    assert_eq!(resp.status(), StatusCode::OK, "upsert failed: {}", resp.text().await.unwrap_or_default());
}

async fn query_ids(base_url: &str, rank_by: Value) -> Vec<String> {
    let body = json!({
        "rank_by": rank_by,
        "top_k": 3
    });
    let resp = reqwest::Client::new()
        .post(format!("{base_url}/v2/namespaces/{NAMESPACE}/query"))
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

async fn assert_search_results(base_url: &str) {
    let vector_ids = query_ids(
        base_url,
        json!(["vector", "ANN", "embedding", [1.0, 0.0, 0.0]]),
    )
    .await;
    assert_eq!(
        vector_ids.first().map(String::as_str),
        Some("doc-a"),
        "vector top-1 should be doc-a, got {vector_ids:?}"
    );

    let fts_ids = query_ids(base_url, json!(["BM25", "text", "alpha"])).await;
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
}

#[tokio::test]
async fn minio_upsert_vector_fts_hybrid_and_restart_persistence() {
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
    assert_search_results(&serve1.base_url).await;

    // Prove data survives serve process restart with only S3 backing.
    serve1.stop();
    drop(serve1);
    sleep(Duration::from_millis(500)).await;

    let serve2 = ServeHandle::spawn(&endpoint, &listen);
    serve2.wait_ready().await;
    assert_search_results(&serve2.base_url).await;
}