//! Optional integration tests against an external S3-compatible backend (MinIO, AWS, etc.).
//!
//! Set `OPENPUFFER_TEST_S3_ENDPOINT` (and optionally bucket/keys), then:
//! `cargo test -F integration --test integration_external_s3 -- --ignored`

mod common;

use common::s3_harness::*;
use serde_json::json;
use std::time::Duration;

const NAMESPACE_EXTERNAL: &str = "itest-external-s3";

#[tokio::test]
#[ignore = "requires OPENPUFFER_TEST_S3_ENDPOINT"]
async fn external_s3_smoke_upsert_wal_bytes_and_query() {
    let fixture = s3_fixture_from_env()
        .await
        .expect("OPENPUFFER_TEST_S3_ENDPOINT must be set for external S3 tests");

    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    upsert_batch(
        &serve.base_url,
        NAMESPACE_EXTERNAL,
        json!([{
            "id": "ext-doc-1",
            "attributes": {
                "text": "external s3 integration smoke",
                "embedding": [1.0, 0.0, 0.0]
            }
        }]),
    )
    .await;

    assert_wal_layout_after_write(&fixture.client, &fixture.bucket, NAMESPACE_EXTERNAL).await;

    let entry =
        decode_wal_entry_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_EXTERNAL, 1).await;
    assert_eq!(wal_upsert_ids(&entry), vec!["ext-doc-1".to_string()]);

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_EXTERNAL).await;
    assert!(meta.wal_commit_seq >= 1);

    let export_ids = export_all_ids(&serve.base_url, NAMESPACE_EXTERNAL, None).await;
    assert!(
        export_ids.contains(&"ext-doc-1".to_string()),
        "export must include ext-doc-1, got {export_ids:?}"
    );

    let fts_ids = query_ids_ns(
        &serve.base_url,
        NAMESPACE_EXTERNAL,
        json!(["BM25", "text", "external"]),
        None,
    )
    .await;
    assert!(
        fts_ids.contains(&"ext-doc-1".to_string()),
        "query must find ext-doc-1, got {fts_ids:?}"
    );

    wait_until_indexed(
        &serve.base_url,
        NAMESPACE_EXTERNAL,
        Duration::from_secs(120),
    )
    .await;
    assert_two_level_centroids_on_backend(
        &fixture.client,
        &fixture.bucket,
        NAMESPACE_EXTERNAL,
    )
    .await;
}