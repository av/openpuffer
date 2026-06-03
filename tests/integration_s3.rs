//! S3 round-trip integration tests against MinIO via testcontainers.
//!
//! Asserts turbopuffer-style layout (`meta.json`, `wal/`, `index/`), background indexing,
//! vector / FTS / hybrid / filter queries, and restart persistence — no `docs/{id}.json`.

mod common;

use common::s3_harness::*;
use openpuffer::models::ROOT_PREFIX;
use reqwest::StatusCode;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;
const NAMESPACE: &str = "itest";
const NAMESPACE_INCR: &str = "itest-incr";
const NAMESPACE_WARM: &str = "itest-warm";
const NAMESPACE_DEL_FILTER: &str = "itest-del-filter";
const NAMESPACE_UUID_FILTER: &str = "itest-uuid-filter";
const NAMESPACE_PATCH_FILTER: &str = "itest-patch-filter";
const NAMESPACE_PATCH: &str = "itest-patch";
const NAMESPACE_CONCURRENT: &str = "itestconcurrent";
const NAMESPACE_RESTART_WRITE: &str = "itest-restart-write";
const NAMESPACE_EXPORT: &str = "itest-export";
const NAMESPACE_WAL_RATE: &str = "itest-wal-rate";
const NAMESPACE_COPY_SRC: &str = "itest-copy-src";
const NAMESPACE_COPY_DEST: &str = "itest-copy-dest";
const NAMESPACE_BRANCH_SRC: &str = "itest-branch-src";
const NAMESPACE_BRANCH_DEST: &str = "itest-branch-dest";
const NAMESPACE_HEALTH_META: &str = "itest-health-meta";
const NAMESPACE_10K: &str = "itest-10k";
const NAMESPACE_WAL_COMPACT: &str = "itest-wal-compact";
const NAMESPACE_UPSERT_COND: &str = "itest-upsert-cond";
const NAMESPACE_DATETIME_UPSERT_COND: &str = "itest-datetime-upsert-cond";
const NAMESPACE_ORDER_BY: &str = "itest-order-by";
const NAMESPACE_QUERY_BILLING: &str = "itest-query-billing";
const NAMESPACE_DISTANCE_METRIC: &str = "itest-distance-metric";
const NAMESPACE_AFFECTED_IDS: &str = "itest-affected-ids";
const NAMESPACE_S3_WAL_BYTES: &str = "itest-s3-wal-bytes";
const NAMESPACE_S3_L1_CENTROIDS: &str = "itest-s3-l1-centroids";
const NAMESPACE_VEC_B64: &str = "itest-vec-b64";
const NAMESPACE_FAIR_HOT: &str = "itest-fair-hot";
const NAMESPACE_FAIR_B: &str = "itest-fair-b";
const NAMESPACE_FAIR_C: &str = "itest-fair-c";
const FAIR_HOT_BATCH: usize = 400;
const FAIR_HOT_BATCHES: usize = 5;
const NAMESPACE_S3_TWO_INST: &str = "itest-s3-two-inst";
const NAMESPACE_S3_COLD_RT: &str = "itest-s3-cold-roundtrips";
const NAMESPACE_S3_COMPACT: &str = "itest-s3-compact";
const NAMESPACE_S3_SEG_GROW: &str = "itest-s3-seg-grow";
const NAMESPACE_S3_COPY_KEYS_SRC: &str = "itest-s3-copy-keys-src";
const NAMESPACE_S3_COPY_KEYS_DEST: &str = "itest-s3-copy-keys-dest";
const NAMESPACE_S3_UPSERT_COND: &str = "itest-s3-upsert-cond";
const NAMESPACE_S3_PATCH_FILTER_WAL: &str = "itest-s3-patch-filter-wal";
const NAMESPACE_S3_BRANCH_SRC: &str = "itest-s3-branch-src";
const NAMESPACE_S3_BRANCH_DEST: &str = "itest-s3-branch-dest";
const NAMESPACE_BB3_COMPACT_EV: &str = "itest-bb3-compact-ev";
const NAMESPACE_BB3_BRANCH_SRC: &str = "itest-bb3-branch-src";
const NAMESPACE_BB3_BRANCH_DEST: &str = "itest-bb3-branch-patch";
const NAMESPACE_BB3_F16_HYBRID: &str = "itest-bb3-f16-hybrid";
const NAMESPACE_BB3_COPY_QUERY_SRC: &str = "itest-bb3-copy-query-src";
const NAMESPACE_BB3_COPY_QUERY_DEST: &str = "itest-bb3-copy-query-dest";
const NAMESPACE_S3_TWO_VEC: &str = "itest-s3-two-vector-fields";
const NAMESPACE_FULL_ARCH: &str = "itest-full-arch";
const NAMESPACE_FULL_ARCH_BRANCH: &str = "itest-full-arch-branch";
/// turbopuffer base64 for `[1.0, 0.0, 0.0]` f32 LE.
const EMB_B64_THREE: &str = "AACAPwAAAAAAAAAA";
const STRESS_DOCS: usize = 10_000;
const STRESS_BATCH: usize = 2_000;
const STRESS_DIM: usize = 128;

fn ns_prefix() -> String {
    format!("{ROOT_PREFIX}{NAMESPACE}/")
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
    let fixture = S3Fixture::from_testcontainers().await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");

    let mut serve1 = ServeHandle::spawn(&fixture, &listen);
    serve1.wait_ready().await;
    upsert_documents(&serve1.base_url).await;

    assert_wal_layout_after_write(&fixture.client, &fixture.bucket, NAMESPACE).await;
    let wal_entry = decode_wal_entry_from_s3(&fixture.client, &fixture.bucket, NAMESPACE, 1).await;
    let wal_ids = wal_upsert_ids(&wal_entry);
    assert_eq!(wal_ids.len(), 3, "wal/00000001.bin should contain 3 upserts");
    assert!(wal_ids.contains(&"doc-a".to_string()));
    assert!(wal_ids.contains(&"doc-b".to_string()));
    assert!(wal_ids.contains(&"doc-c".to_string()));

    wait_until_indexed(&serve1.base_url, NAMESPACE, Duration::from_secs(30)).await;
    assert_index_objects(&fixture.client, &fixture.bucket, NAMESPACE).await;

    assert_search_results(&serve1.base_url).await;

    // Prove data survives serve process restart with only S3 backing (WAL + index).
    serve1.stop();
    drop(serve1);
    sleep(Duration::from_millis(500)).await;

    let serve2 = ServeHandle::spawn(&fixture, &listen);
    serve2.wait_ready().await;

    assert_wal_layout_after_write(&fixture.client, &fixture.bucket, NAMESPACE).await;
    wait_until_indexed(&serve2.base_url, NAMESPACE, Duration::from_secs(10)).await;
    assert_index_objects(&fixture.client, &fixture.bucket, NAMESPACE).await;
    assert_search_results(&serve2.base_url).await;

    // Namespace prefix must not contain legacy doc JSON paths.
    let all_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &ns_prefix()).await;
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
    let fixture = S3Fixture::from_testcontainers().await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
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
    wait_until_indexed(&serve.base_url, NAMESPACE_INCR, Duration::from_secs(30)).await;

    let meta1 = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_INCR).await;
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
    wait_until_indexed(&serve.base_url, NAMESPACE_INCR, Duration::from_secs(30)).await;

    let meta2 = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_INCR).await;
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
    wait_until_indexed(&serve.base_url, NAMESPACE_INCR, Duration::from_secs(30)).await;

    let meta3 = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_INCR).await;
    assert_eq!(meta3.index_cursor, 3);
    assert_eq!(meta3.wal_commit_seq, 3);
    assert_eq!(meta3.fts_segment_ids, vec![1, 2, 3]);
    assert_eq!(meta3.filter_segment_ids, vec![1, 2, 3]);
    assert_eq!(meta3.vector_segment_ids, vec![1, 2, 3]);

    for seq in 1..=3 {
        let entry =
            decode_wal_entry_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_INCR, seq).await;
        assert_eq!(
            wal_upsert_ids(&entry),
            vec![format!("batch-{seq}")],
            "wal segment {seq} should contain exactly one upsert"
        );
    }

    let index_prefix = format!("{ROOT_PREFIX}{NAMESPACE_INCR}/index/");
    let keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &index_prefix).await;
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
    let fixture = S3Fixture::from_testcontainers().await;

    let cache_dir = tempfile::tempdir().expect("cache tempdir");
    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn_with_cache(
        &fixture,
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
    wait_until_indexed(&serve.base_url, NAMESPACE_WARM, Duration::from_secs(30)).await;

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

/// Warm + eventual consistency: no S3 GetObject on query even with unindexed WAL tail after warm.
#[tokio::test]
async fn warm_eventual_query_zero_s3_gets_with_unindexed_tail() {
    let fixture = S3Fixture::from_testcontainers().await;

    let cache_dir = tempfile::tempdir().expect("cache tempdir");
    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn_with_cache(
        &fixture,
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
    wait_until_indexed(&serve.base_url, NAMESPACE_WARM, Duration::from_secs(30)).await;

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

    // Create unindexed WAL tail (strong would scan these segments from S3).
    upsert_batch(
        &serve.base_url,
        NAMESPACE_WARM,
        json!([{
            "id": "post-warm-unindexed",
            "attributes": {
                "embedding": [0.0, 1.0, 0.0],
                "text": "written after warm not yet indexed",
                "tier": "free"
            }
        }]),
    )
    .await;

    let reset = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");
    assert_eq!(reset.status(), StatusCode::OK);

    let body = json!({
        "rank_by": ["BM25", "text", "warm"],
        "top_k": 3,
        "consistency": "eventual"
    });
    let resp = query_response_ns(&serve.base_url, NAMESPACE_WARM, body).await;
    let ids: Vec<String> = resp["rows"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        ids.contains(&"warm-doc".to_string()),
        "eventual query should hit indexed warm-doc, got {ids:?}"
    );
    assert!(
        !ids.contains(&"post-warm-unindexed".to_string()),
        "eventual query must not see unindexed tail doc"
    );

    let stats = client
        .get(format!("{}/v1/debug/cache-stats", serve.base_url))
        .send()
        .await
        .expect("cache stats");
    let stats_body: Value = stats.json().await.expect("stats json");
    assert_eq!(
        stats_body["s3_get_count"].as_u64(),
        Some(0),
        "warm + eventual query should not S3 GetObject (disk cache + no WAL tail)"
    );
}

/// Schema on write persists in meta; delete_by_filter removes matching docs from queries.
#[tokio::test]
async fn schema_on_write_and_delete_by_filter() {
    let fixture = S3Fixture::from_testcontainers().await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
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
    wait_until_indexed(&serve.base_url, NAMESPACE_DEL_FILTER, Duration::from_secs(30)).await;

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_DEL_FILTER).await;
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

/// Schema `uuid` validates on write (canonical string) and filter index supports Eq.
#[tokio::test]
async fn filter_uuid_eq() {
    let fixture = S3Fixture::from_testcontainers().await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    const UUID_A: &str = "550e8400-e29b-41d4-a716-446655440001";
    const UUID_B: &str = "550e8400-e29b-41d4-a716-446655440002";

    write_batch(
        &serve.base_url,
        NAMESPACE_UUID_FILTER,
        json!({
            "schema": {
                "text": {"type": "string", "full_text_search": true},
                "tenant_id": "uuid",
                "permissions": "[]uuid"
            },
            "upsert_rows": [
                {
                    "id": "doc-a",
                    "attributes": {
                        "text": "uuid tenant alpha",
                        "tenant_id": "550E8400-E29B-41D4-A716-446655440001",
                        "permissions": ["550e8400e29b41d4a716446655440010"]
                    }
                },
                {
                    "id": "doc-b",
                    "attributes": {
                        "text": "uuid tenant beta",
                        "tenant_id": "550e8400-e29b-41d4-a716-446655440002",
                        "permissions": []
                    }
                }
            ]
        }),
    )
    .await;
    wait_until_indexed(&serve.base_url, NAMESPACE_UUID_FILTER, Duration::from_secs(30)).await;

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_UUID_FILTER).await;
    assert_eq!(meta.schema["tenant_id"], json!("uuid"));

    let wal_entry = decode_wal_entry_from_s3(
        &fixture.client,
        &fixture.bucket,
        NAMESPACE_UUID_FILTER,
        1,
    )
    .await;
    let wal_docs = wal_entry.into_documents().expect("decode WAL documents");
    let doc_a = wal_docs
        .iter()
        .find(|d| d.id == "doc-a")
        .expect("doc-a in WAL");
    assert_eq!(
        doc_a.attributes.get("tenant_id").and_then(|v| v.as_str()),
        Some(UUID_A),
        "uuid must be stored as canonical lowercase string"
    );
    assert_eq!(
        doc_a.attributes.get("permissions").and_then(|v| v.as_array()),
        Some(&vec![json!("550e8400-e29b-41d4-a716-446655440010")]),
        "[]uuid elements stored as canonical strings"
    );

    let filter_a = query_ids_ns(
        &serve.base_url,
        NAMESPACE_UUID_FILTER,
        json!(["BM25", "text", "tenant"]),
        Some(json!(["tenant_id", "Eq", UUID_A])),
    )
    .await;
    assert_eq!(
        filter_a,
        vec!["doc-a".to_string()],
        "Eq filter on uuid tenant_id should return doc-a only, got {filter_a:?}"
    );

    let filter_b = query_ids_ns(
        &serve.base_url,
        NAMESPACE_UUID_FILTER,
        json!(["BM25", "text", "tenant"]),
        Some(json!(["tenant_id", "Eq", UUID_B])),
    )
    .await;
    assert_eq!(
        filter_b,
        vec!["doc-b".to_string()],
        "Eq filter on uuid tenant_id should return doc-b only, got {filter_b:?}"
    );

    let client = reqwest::Client::new();
    let bad = client
        .post(format!(
            "{}/v2/namespaces/{}",
            serve.base_url, NAMESPACE_UUID_FILTER
        ))
        .json(&json!({
            "upsert_rows": [{
                "id": "doc-bad",
                "attributes": {
                    "text": "invalid uuid",
                    "tenant_id": "not-a-uuid"
                }
            }]
        }))
        .send()
        .await
        .expect("invalid uuid upsert");
    assert_eq!(
        bad.status(),
        StatusCode::BAD_REQUEST,
        "invalid uuid must be rejected: {}",
        bad.text().await.unwrap_or_default()
    );
}

/// patch_by_filter resolves ids via filter index + WAL tail and merges patch attrs in WAL.
#[tokio::test]
async fn patch_by_filter_updates_matching_docs() {
    let fixture = S3Fixture::from_testcontainers().await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_PATCH_FILTER,
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
    wait_until_indexed(
        &serve.base_url,
        NAMESPACE_PATCH_FILTER,
        Duration::from_secs(30),
    )
    .await;

    let client = reqwest::Client::new();
    let write_url = format!(
        "{}/v2/namespaces/{}",
        serve.base_url, NAMESPACE_PATCH_FILTER
    );
    let resp = client
        .post(&write_url)
        .json(&json!({
            "patch_by_filter": {
                "filters": ["tier", "Eq", "free"],
                "patch": { "tier": "upgraded", "text": "charlie patched unique" }
            }
        }))
        .send()
        .await
        .expect("patch_by_filter write");
    assert_eq!(resp.status(), StatusCode::OK, "patch_by_filter failed");
    let first_body: Value = resp.json().await.expect("write json");
    assert_eq!(
        first_body["rows_patched"].as_u64(),
        Some(1),
        "one free-tier doc patched: {first_body}"
    );

    sleep(Duration::from_millis(1500)).await;

    let upgraded = query_ids_ns(
        &serve.base_url,
        NAMESPACE_PATCH_FILTER,
        json!(["vector", "ANN", "embedding", [0.0, 1.0, 0.0]]),
        Some(json!(["tier", "Eq", "upgraded"])),
    )
    .await;
    assert!(
        upgraded.contains(&"doc-b".to_string()),
        "doc-b should match upgraded tier after patch_by_filter, got {upgraded:?}"
    );

    let still_pro = query_ids_ns(
        &serve.base_url,
        NAMESPACE_PATCH_FILTER,
        json!(["vector", "ANN", "embedding", [1.0, 0.0, 0.0]]),
        Some(json!(["tier", "Eq", "pro"])),
    )
    .await;
    assert!(
        still_pro.contains(&"doc-a".to_string()),
        "doc-a (pro) must be unchanged, got {still_pro:?}"
    );

    let free_gone = query_ids_ns(
        &serve.base_url,
        NAMESPACE_PATCH_FILTER,
        json!(["vector", "ANN", "embedding", [0.0, 1.0, 0.0]]),
        Some(json!(["tier", "Eq", "free"])),
    )
    .await;
    assert!(
        free_gone.is_empty(),
        "free tier filter must not match after patch, got {free_gone:?}"
    );

    let resp = client
        .post(&write_url)
        .json(&json!({
            "patch_by_filter": {
                "filters": ["tier", "Eq", "free"],
                "patch": { "tier": "noop" }
            }
        }))
        .send()
        .await
        .expect("patch_by_filter recount");
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.expect("write json");
    assert_eq!(
        body.get("rows_patched").and_then(|v| v.as_u64()).unwrap_or(0),
        0,
        "no free-tier docs left to patch: {body}"
    );
    assert_eq!(body["rows_affected"].as_u64(), Some(0));

    let bad = client
        .post(&write_url)
        .json(&json!({
            "patch_by_filter": {
                "filters": ["tier", "Eq", "upgraded"],
                "patch": { "embedding": [0.0, 0.0, 1.0] }
            }
        }))
        .send()
        .await
        .expect("patch vector by filter");
    assert_eq!(
        bad.status(),
        StatusCode::BAD_REQUEST,
        "vector patch in patch_by_filter must return 400: {}",
        bad.text().await.unwrap_or_default()
    );
}

/// patch_rows merges attributes in WAL; patched text is visible in FTS after indexing.
#[tokio::test]
async fn patch_rows_updates_fts_after_index() {
    let fixture = S3Fixture::from_testcontainers().await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
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
    wait_until_indexed(&serve.base_url, NAMESPACE_PATCH, Duration::from_secs(30)).await;

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
    wait_until_indexed(&serve.base_url, NAMESPACE_PATCH, Duration::from_secs(30)).await;

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
    let fixture = S3Fixture::from_testcontainers().await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn_with_options(&fixture, &listen, None, Some(1), None);
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
    let wal_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &wal_prefix).await;
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

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_WAL_RATE).await;
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
        &fixture.client,
        &fixture.bucket,
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
    let fixture = S3Fixture::from_testcontainers().await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
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

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_CONCURRENT).await;
    assert!(
        meta.wal_commit_seq >= 1 && meta.wal_commit_seq <= 10,
        "wal_commit_seq must advance monotonically (1..=10 commits), meta={meta:?}"
    );

    let wal_prefix = format!("{ROOT_PREFIX}{NAMESPACE_CONCURRENT}/wal/");
    let wal_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &wal_prefix).await;
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
        &fixture.client,
        &fixture.bucket,
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
    let fixture = S3Fixture::from_testcontainers().await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");

    let mut serve1 = ServeHandle::spawn(&fixture, &listen);
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
    wait_until_indexed(&serve1.base_url, NAMESPACE_RESTART_WRITE, Duration::from_secs(30))
        .await;

    serve1.stop();
    drop(serve1);
    sleep(Duration::from_millis(500)).await;

    let serve2 = ServeHandle::spawn(&fixture, &listen);
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
/// Export reconstructs all document ids from WAL snapshot (paginated `last_id`).
#[tokio::test]
async fn export_after_writes_returns_all_doc_ids() {
    let fixture = S3Fixture::from_testcontainers().await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
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

    let wal_entry =
        decode_wal_entry_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_EXPORT, 1).await;
    let mut wal_ids = wal_upsert_ids(&wal_entry);
    wal_ids.sort();
    assert_eq!(
        wal_ids,
        vec!["exp-a".to_string(), "exp-b".to_string(), "exp-c".to_string()],
        "S3 wal bytes must list all upserted doc ids"
    );

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

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_EXPORT).await;
    assert_eq!(meta.wal_commit_seq, commit_seq);
}

/// `copy_from_namespace` clones S3 layout; query on destination returns same documents.
#[tokio::test]
async fn copy_from_namespace_returns_same_docs_on_dest() {
    let fixture = S3Fixture::from_testcontainers().await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
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
    wait_until_indexed(&serve.base_url, NAMESPACE_COPY_SRC, Duration::from_secs(90)).await;

    write_batch(
        &serve.base_url,
        NAMESPACE_COPY_DEST,
        json!({"copy_from_namespace": NAMESPACE_COPY_SRC}),
    )
    .await;

    let src_meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_COPY_SRC).await;
    let dest_meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_COPY_DEST).await;
    assert_eq!(
        dest_meta.wal_commit_seq, src_meta.wal_commit_seq,
        "dest should inherit WAL commit seq from source"
    );

    let dest_prefix = format!("{ROOT_PREFIX}{NAMESPACE_COPY_DEST}/");
    let dest_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &dest_prefix).await;
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

/// `branch_from_namespace` S3-clones source; writes on branch do not affect source.
#[tokio::test]
async fn branch_from_namespace_independent_writes_do_not_affect_source() {
    let fixture = S3Fixture::from_testcontainers().await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    upsert_batch(
        &serve.base_url,
        NAMESPACE_BRANCH_SRC,
        json!([
            {"id": "branch-src", "attributes": {"text": "branch source only", "embedding": [1.0, 0.0, 0.0]}},
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, NAMESPACE_BRANCH_SRC, Duration::from_secs(90)).await;

    write_batch(
        &serve.base_url,
        NAMESPACE_BRANCH_DEST,
        json!({"branch_from_namespace": NAMESPACE_BRANCH_SRC}),
    )
    .await;

    upsert_batch(
        &serve.base_url,
        NAMESPACE_BRANCH_DEST,
        json!([
            {"id": "branch-only", "attributes": {"text": "branch dest exclusive", "embedding": [0.0, 1.0, 0.0]}},
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, NAMESPACE_BRANCH_DEST, Duration::from_secs(90)).await;

    let dest_fts = query_ids_ns(
        &serve.base_url,
        NAMESPACE_BRANCH_DEST,
        json!(["BM25", "text", "exclusive"]),
        None,
    )
    .await;
    assert!(
        dest_fts.iter().any(|id| id == "branch-only"),
        "branch dest should find branch-only doc, got {dest_fts:?}"
    );

    let src_fts_exclusive = query_ids_ns(
        &serve.base_url,
        NAMESPACE_BRANCH_SRC,
        json!(["BM25", "text", "exclusive"]),
        None,
    )
    .await;
    assert!(
        !src_fts_exclusive.iter().any(|id| id == "branch-only"),
        "source must not see branch-only doc, got {src_fts_exclusive:?}"
    );

    let src_fts_original = query_ids_ns(
        &serve.base_url,
        NAMESPACE_BRANCH_SRC,
        json!(["BM25", "text", "source"]),
        None,
    )
    .await;
    assert!(
        src_fts_original.iter().any(|id| id == "branch-src"),
        "source should still have branch-src, got {src_fts_original:?}"
    );

    let dest_prefix = format!("{ROOT_PREFIX}{NAMESPACE_BRANCH_DEST}/");
    let dest_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &dest_prefix).await;
    assert!(
        dest_keys.iter().any(|k| k.contains("/wal/")),
        "branch dest missing wal objects: {dest_keys:?}"
    );
}

#[tokio::test]
async fn deep_health_and_namespace_metadata_fields() {
    let fixture = S3Fixture::from_testcontainers().await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let mut serve = ServeHandle::spawn(&fixture, &listen);
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
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(90)).await;

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
    let fixture = S3Fixture::from_testcontainers().await;

    let cache_dir = tempfile::tempdir().expect("cache tempdir");
    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn_with_options(
        &fixture,
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

    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(180)).await;
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

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert_eq!(meta.index_cursor, meta.wal_commit_seq);
    assert!(meta.wal_commit_seq >= 1);

    let l0_key = format!("{ROOT_PREFIX}{ns}/index/embedding/centroids-l0.bin");
    let l0_size = object_size(&fixture.client, &fixture.bucket, &l0_key).await;
    assert!(
        l0_size > 0,
        "centroids-l0.bin must exist and be non-empty on MinIO after 10k index, size={l0_size}"
    );

    eprintln!(
        "ten_thousand_docs_indexed_query: writes={write_elapsed:?} index+query={:?} wal_commits={} l0_bytes={l0_size}",
        index_elapsed,
        meta.wal_commit_seq
    );
    assert!(
        test_started.elapsed() < Duration::from_secs(180),
        "test exceeded 180s wall clock"
    );
}

/// Fifteen WAL commits → indexer catches up → compaction deletes indexed segments; cold query still works.
#[tokio::test]
async fn wal_compaction_after_full_index_query_still_works() {
    let fixture = S3Fixture::from_testcontainers().await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let mut serve = ServeHandle::spawn_with_options(
        &fixture,
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

    wait_until_indexed(
        &serve.base_url,
        NAMESPACE_WAL_COMPACT,
        Duration::from_secs(90),
    )
    .await;

    let snapshot_key = format!("{ROOT_PREFIX}{NAMESPACE_WAL_COMPACT}/wal/snapshot.bin");
    let wal_prefix = format!("{ROOT_PREFIX}{NAMESPACE_WAL_COMPACT}/wal/");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_WAL_COMPACT).await;
    loop {
        let wal_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &wal_prefix).await;
        let segment_wals: Vec<_> = wal_keys
            .iter()
            .filter(|k| {
                k.starts_with(&wal_prefix)
                    && k.ends_with(".bin")
                    && !k.ends_with("snapshot.bin")
            })
            .collect();
        let compacted = meta.wal_snapshot_seq > 0
            && meta.wal_snapshot_seq >= meta.index_cursor
            && s3_object_exists(&fixture.client, &fixture.bucket, &snapshot_key).await
            && segment_wals.len() <= 3;
        if compacted {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            let mut missing = Vec::new();
            for seq in 1..=meta.wal_commit_seq {
                let key = format!("{wal_prefix}{seq:08}.bin");
                if !s3_object_exists(&fixture.client, &fixture.bucket, &key).await {
                    missing.push(seq);
                }
            }
            panic!(
                "wal compaction did not finish within 60s, meta={meta:?} wal_keys={wal_keys:?} segment_wals={segment_wals:?} missing_seqs={missing:?}"
            );
        }
        sleep(Duration::from_millis(250)).await;
        meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_WAL_COMPACT).await;
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

    let wal_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &wal_prefix).await;
    assert!(
        wal_keys.iter().any(|k| k == &snapshot_key),
        "expected wal/snapshot.bin, keys={wal_keys:?}"
    );

    let first_wal = format!("{wal_prefix}00000001.bin");
    assert!(
        !s3_object_exists(&fixture.client, &fixture.bucket, &first_wal).await,
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
        &fixture,
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

/// Schema `datetime` + `upsert_condition` newer-timestamp pattern with `$ref_new`.
#[tokio::test]
async fn upsert_condition_newer_timestamp_with_datetime() {
    let fixture = S3Fixture::from_testcontainers().await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    const T1: &str = "2024-06-01T12:00:00.000000000Z";
    const T2: &str = "2024-12-01T12:00:00.000000000Z";
    const T0: &str = "2024-01-01T00:00:00.000000000Z";

    let newer_ts_condition = json!([
        "Or",
        [
            ["updated_at", "Lt", {"$ref_new": "updated_at"}],
            ["updated_at", "Eq", null]
        ]
    ]);

    write_batch(
        &serve.base_url,
        NAMESPACE_DATETIME_UPSERT_COND,
        json!({
            "schema": {
                "title": {
                    "type": "string",
                    "filterable": true,
                    "full_text_search": true
                },
                "updated_at": "datetime"
            },
            "upsert_rows": [{
                "id": "doc-1",
                "attributes": {
                    "title": "v1",
                    "updated_at": "2024-06-01T12:00:00Z"
                }
            }]
        }),
    )
    .await;
    sleep(Duration::from_millis(1200)).await;

    let http = reqwest::Client::new();
    let write_url = format!(
        "{}/v2/namespaces/{NAMESPACE_DATETIME_UPSERT_COND}",
        serve.base_url
    );

    let resp_newer = http
        .post(&write_url)
        .json(&json!({
            "upsert_condition": newer_ts_condition,
            "upsert_rows": [{
                "id": "doc-1",
                "attributes": {
                    "title": "v2",
                    "updated_at": "2024-12-01T12:00:00Z"
                }
            }]
        }))
        .send()
        .await
        .expect("newer conditional upsert");
    assert_eq!(resp_newer.status(), StatusCode::OK);
    let body_newer: Value = resp_newer.json().await.expect("write json");
    assert_eq!(
        body_newer["rows_upserted"].as_u64(),
        Some(1),
        "newer timestamp must apply, body={body_newer}"
    );

    sleep(Duration::from_millis(1200)).await;

    let resp_older = http
        .post(&write_url)
        .json(&json!({
            "upsert_condition": newer_ts_condition,
            "upsert_rows": [{
                "id": "doc-1",
                "attributes": {
                    "title": "stale",
                    "updated_at": "2024-01-01T00:00:00Z"
                }
            }]
        }))
        .send()
        .await
        .expect("older conditional upsert");
    assert_eq!(resp_older.status(), StatusCode::OK);
    let body_older: Value = resp_older.json().await.expect("write json");
    assert_eq!(
        body_older["rows_upserted"].as_u64().unwrap_or(0),
        0,
        "older timestamp must be skipped, body={body_older}"
    );
    assert_eq!(body_older["rows_affected"].as_u64(), Some(0));

    sleep(Duration::from_millis(1200)).await;

    let export = http
        .get(format!(
            "{}/v1/namespaces/{NAMESPACE_DATETIME_UPSERT_COND}/export",
            serve.base_url
        ))
        .send()
        .await
        .expect("export");
    assert_eq!(export.status(), StatusCode::OK);
    let exported: Value = export.json().await.expect("export json");
    let row = exported["rows"]
        .as_array()
        .and_then(|r| r.first())
        .expect("one row");
    assert_eq!(row["id"], "doc-1");
    assert_eq!(row["attributes"]["title"], "v2");
    assert_eq!(row["attributes"]["updated_at"], T2);

    let wal_entry = decode_wal_entry_from_s3(
        &fixture.client,
        &fixture.bucket,
        NAMESPACE_DATETIME_UPSERT_COND,
        1,
    )
    .await;
    let wal_docs = wal_entry.into_documents().expect("decode WAL");
    let doc = wal_docs
        .iter()
        .find(|d| d.id == "doc-1")
        .expect("doc-1 in WAL");
    assert_eq!(
        doc.attributes.get("updated_at").and_then(|v| v.as_str()),
        Some(T1),
        "WAL must store canonical datetime"
    );

    wait_until_indexed(
        &serve.base_url,
        NAMESPACE_DATETIME_UPSERT_COND,
        Duration::from_secs(30),
    )
    .await;

    let filter_gt = query_ids_ns(
        &serve.base_url,
        NAMESPACE_DATETIME_UPSERT_COND,
        json!(["BM25", "title", "v2"]),
        Some(json!(["updated_at", "Gt", T1])),
    )
    .await;
    assert_eq!(
        filter_gt,
        vec!["doc-1".to_string()],
        "datetime Gt filter should match doc-1, got {filter_gt:?}"
    );

    let filter_lt = query_ids_ns(
        &serve.base_url,
        NAMESPACE_DATETIME_UPSERT_COND,
        json!(["BM25", "title", "v2"]),
        Some(json!(["updated_at", "Lt", "2025-01-01T00:00:00.000000000Z"])),
    )
    .await;
    assert_eq!(
        filter_lt,
        vec!["doc-1".to_string()],
        "datetime Lt filter should match doc-1, got {filter_lt:?}"
    );

    let bad = http
        .post(&write_url)
        .json(&json!({
            "upsert_rows": [{
                "id": "doc-bad",
                "attributes": {
                    "title": "bad time",
                    "updated_at": "yesterday"
                }
            }]
        }))
        .send()
        .await
        .expect("invalid datetime upsert");
    assert_eq!(
        bad.status(),
        StatusCode::BAD_REQUEST,
        "invalid datetime must be rejected: {}",
        bad.text().await.unwrap_or_default()
    );
}

/// `upsert_condition` with `["id","Eq",null]`: insert new ids, skip overwrites.
#[tokio::test]
async fn upsert_condition_insert_if_not_exists() {
    let fixture = S3Fixture::from_testcontainers().await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
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

/// Query responses expose `performance.billing` logical-byte estimates.
#[tokio::test]
async fn query_performance_billing_fields_smoke() {
    let fixture = S3Fixture::from_testcontainers().await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_QUERY_BILLING,
        json!({
            "upsert_rows": [
                {
                    "id": "qb-1",
                    "attributes": {
                        "text": "billing smoke document one",
                        "embedding": [1.0, 0.0, 0.0]
                    }
                },
                {
                    "id": "qb-2",
                    "attributes": {
                        "text": "billing smoke document two",
                        "embedding": [0.0, 1.0, 0.0]
                    }
                }
            ]
        }),
    )
    .await;
    sleep(Duration::from_millis(1200)).await;

    let v = query_response_ns(
        &serve.base_url,
        NAMESPACE_QUERY_BILLING,
        json!({
            "rank_by": ["BM25", "text", "billing smoke"],
            "top_k": 2,
            "include_attributes": true
        }),
    )
    .await;
    let billing = v["performance"]["billing"]
        .as_object()
        .expect("performance.billing");
    let queried = billing["billable_logical_bytes_queried"]
        .as_u64()
        .expect("billable_logical_bytes_queried");
    let returned = billing["billable_logical_bytes_returned"]
        .as_u64()
        .expect("billable_logical_bytes_returned");
    assert!(queried > 0, "queried bytes should be positive");
    assert!(returned > 0, "returned bytes should be positive");
    assert!(queried >= returned, "queried >= returned for top_k=2");
}

/// `order_by` breaks ties after `rank_by` scoring (turbopuffer attribute sort shape).
#[tokio::test]
async fn order_by_sorts_tied_bm25_results_by_attribute() {
    let fixture = S3Fixture::from_testcontainers().await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
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

/// `distance_metric` on first write is stored in meta; conflicting later write is rejected.
#[tokio::test]
async fn distance_metric_stored_and_enforced_on_write() {
    let fixture = S3Fixture::from_testcontainers().await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_DISTANCE_METRIC,
        json!({
            "distance_metric": "euclidean_squared",
            "upsert_rows": [
                { "id": "dm-a", "attributes": { "embedding": [0.0, 0.0] } },
                { "id": "dm-b", "attributes": { "embedding": [3.0, 4.0] } }
            ],
            "schema": { "embedding": "[2]f32" }
        }),
    )
    .await;
    sleep(Duration::from_millis(1200)).await;

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_DISTANCE_METRIC).await;
    assert_eq!(
        meta.distance_metric,
        openpuffer::meta::DistanceMetric::EuclideanSquared
    );

    let client = reqwest::Client::new();
    let conflict = client
        .post(format!(
            "{}/v2/namespaces/{}",
            serve.base_url, NAMESPACE_DISTANCE_METRIC
        ))
        .json(&json!({
            "distance_metric": "cosine_distance",
            "upsert_rows": [{ "id": "dm-c", "attributes": { "embedding": [1.0, 0.0] } }]
        }))
        .send()
        .await
        .expect("conflict write");
    assert_eq!(
        conflict.status(),
        StatusCode::BAD_REQUEST,
        "conflicting distance_metric must be rejected: {}",
        conflict.text().await.unwrap_or_default()
    );

    wait_until_indexed(&serve.base_url, NAMESPACE_DISTANCE_METRIC, Duration::from_secs(90))
        .await;
    let v = query_response_ns(
        &serve.base_url,
        NAMESPACE_DISTANCE_METRIC,
        json!({
            "rank_by": ["vector", "ANN", "embedding", [0.0, 0.0]],
            "top_k": 2
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
        ids.first().map(String::as_str),
        Some("dm-a"),
        "euclidean_squared should rank [0,0] nearest to query [0,0], got {ids:?}"
    );
}

/// `return_affected_ids` returns upserted and deleted id lists for the write batch.
#[tokio::test]
async fn return_affected_ids_lists_upserts_and_deletes() {
    let fixture = S3Fixture::from_testcontainers().await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{}/v2/namespaces/{}",
            serve.base_url, NAMESPACE_AFFECTED_IDS
        ))
        .json(&json!({
            "return_affected_ids": true,
            "upsert_rows": [
                { "id": "aff-1", "attributes": { "text": "one" } },
                { "id": "aff-2", "attributes": { "text": "two" } }
            ],
            "deletes": ["aff-ghost"]
        }))
        .send()
        .await
        .expect("write");
    assert_eq!(resp.status(), StatusCode::OK);
    let v: Value = resp.json().await.expect("write json");
    let upserted: Vec<String> = v["upserted_ids"]
        .as_array()
        .expect("upserted_ids")
        .iter()
        .map(|x| x.as_str().expect("id").to_string())
        .collect();
    let deleted: Vec<String> = v["deleted_ids"]
        .as_array()
        .expect("deleted_ids")
        .iter()
        .map(|x| x.as_str().expect("id").to_string())
        .collect();
    assert_eq!(upserted, vec!["aff-1".to_string(), "aff-2".to_string()]);
    assert_eq!(deleted, vec!["aff-ghost".to_string()]);
}

/// After upsert, S3 `wal/00000001.bin` bincode must match HTTP export doc ids.
#[tokio::test]
async fn s3_wal_bytes_match_http_write() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    upsert_batch(
        &serve.base_url,
        NAMESPACE_S3_WAL_BYTES,
        json!([
            {"id": "wal-a", "attributes": {"text": "alpha s3 wal", "embedding": [1.0, 0.0, 0.0]}},
            {"id": "wal-b", "attributes": {"text": "beta s3 wal", "embedding": [0.0, 1.0, 0.0]}},
        ]),
    )
    .await;

    assert_key_exists(
        &fixture.client,
        &fixture.bucket,
        &openpuffer::wal::wal_key(NAMESPACE_S3_WAL_BYTES, 1),
    )
    .await;

    let entry =
        decode_wal_entry_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_S3_WAL_BYTES, 1)
            .await;
    let mut wal_ids = wal_upsert_ids(&entry);
    wal_ids.sort();

    let export_ids = export_all_ids(&serve.base_url, NAMESPACE_S3_WAL_BYTES, None).await;
    assert_eq!(wal_ids, export_ids, "wal bincode ids must match HTTP export");

    let fts_ids = query_ids_ns(
        &serve.base_url,
        NAMESPACE_S3_WAL_BYTES,
        json!(["BM25", "text", "alpha"]),
        None,
    )
    .await;
    assert!(
        fts_ids.contains(&"wal-a".to_string()),
        "query should see wal-a after S3-backed write, got {fts_ids:?}"
    );
}

/// After indexing, S3 must contain non-empty L0 and L1 centroid segments.
#[tokio::test]
async fn s3_two_level_centroids_exist_on_backend() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    upsert_batch(
        &serve.base_url,
        NAMESPACE_S3_L1_CENTROIDS,
        json!([
            {"id": "c0", "attributes": {"text": "centroid zero", "embedding": [1.0, 0.0, 0.0]}},
            {"id": "c1", "attributes": {"text": "centroid one", "embedding": [0.0, 1.0, 0.0]}},
            {"id": "c2", "attributes": {"text": "centroid two", "embedding": [0.0, 0.0, 1.0]}},
            {"id": "c3", "attributes": {"text": "centroid three", "embedding": [0.5, 0.5, 0.0]}},
        ]),
    )
    .await;

    wait_until_indexed(
        &serve.base_url,
        NAMESPACE_S3_L1_CENTROIDS,
        Duration::from_secs(60),
    )
    .await;

    assert_two_level_centroids_on_backend(
        &fixture.client,
        &fixture.bucket,
        NAMESPACE_S3_L1_CENTROIDS,
    )
    .await;

    let keys = list_namespace_keys(&fixture.client, &fixture.bucket, NAMESPACE_S3_L1_CENTROIDS).await;
    assert!(
        keys.iter().any(|k| k.contains("/index/fts-")),
        "indexed namespace should have fts segments on S3, keys={keys:?}"
    );
}

/// `upsert_condition` skips existing ids before WAL flush; second S3 WAL segment contains only new doc.
#[tokio::test]
async fn s3_upsert_condition_writes_single_wal_entry_on_minio() {
    let fixture = S3Fixture::from_testcontainers().await;
    let ns = NAMESPACE_S3_UPSERT_COND;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        ns,
        json!({
            "upsert_rows": [{
                "id": "doc-a",
                "attributes": { "name": "original", "text": "s3 upsert cond alpha" }
            }]
        }),
    )
    .await;
    sleep(Duration::from_millis(1200)).await;

    let http = reqwest::Client::new();
    let resp = http
        .post(format!("{}/v2/namespaces/{ns}", serve.base_url))
        .json(&json!({
            "upsert_condition": ["id", "Eq", null],
            "upsert_rows": [
                { "id": "doc-a", "attributes": { "name": "should-not-apply" } },
                { "id": "doc-b", "attributes": { "name": "inserted", "text": "s3 upsert cond bravo" } }
            ]
        }))
        .send()
        .await
        .expect("conditional upsert");
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.expect("write json");
    assert_eq!(body["rows_upserted"].as_u64(), Some(1), "body={body}");

    sleep(Duration::from_millis(1200)).await;

    let wal_keys = list_wal_keys(&fixture.client, &fixture.bucket, ns).await;
    let seqs = wal_segment_seqs(&wal_keys);
    assert_eq!(seqs, vec![1, 2], "minimal WAL commits: keys={wal_keys:?}");

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert_eq!(meta.wal_commit_seq, 2);
    assert_eq!(seqs.len(), meta.wal_commit_seq as usize);

    let first =
        decode_wal_entry_from_s3(&fixture.client, &fixture.bucket, ns, 1).await;
    assert_eq!(wal_upsert_ids(&first), vec!["doc-a".to_string()]);

    let second =
        decode_wal_entry_from_s3(&fixture.client, &fixture.bucket, ns, 2).await;
    assert_eq!(
        wal_upsert_ids(&second),
        vec!["doc-b".to_string()],
        "conditional write must skip doc-a in WAL batch: entry={second:?}"
    );
    assert!(
        second.deletes.is_empty() && second.patches.is_empty(),
        "second WAL entry should be upsert-only: {second:?}"
    );
}

/// `patch_by_filter` persists matching doc patches in S3 WAL bincode.
#[tokio::test]
async fn s3_patch_by_filter_persists_in_wal_bin() {
    let fixture = S3Fixture::from_testcontainers().await;
    let ns = NAMESPACE_S3_PATCH_FILTER_WAL;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        ns,
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
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    let resp = reqwest::Client::new()
        .post(format!("{}/v2/namespaces/{ns}", serve.base_url))
        .json(&json!({
            "patch_by_filter": {
                "filters": ["tier", "Eq", "free"],
                "patch": { "tier": "upgraded", "text": "charlie patched unique" }
            }
        }))
        .send()
        .await
        .expect("patch_by_filter");
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.expect("write json");
    assert_eq!(body["rows_patched"].as_u64(), Some(1));

    sleep(Duration::from_millis(1200)).await;

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    let seq = meta.wal_commit_seq;
    assert!(seq >= 2, "patch must commit a new WAL segment, meta={meta:?}");

    let entry = decode_wal_entry_from_s3(&fixture.client, &fixture.bucket, ns, seq).await;
    assert_eq!(wal_patch_ids(&entry), vec!["doc-b".to_string()]);
    assert!(entry.upserts.is_empty(), "patch_by_filter WAL should not upsert: {entry:?}");

    let patches = entry.patch_documents().expect("decode WalPatch rows");
    let doc_b = patches
        .iter()
        .find(|p| p.id == "doc-b")
        .expect("doc-b patch in WAL");
    assert_eq!(
        doc_b.attributes.get("tier").and_then(|v| v.as_str()),
        Some("upgraded")
    );
    assert_eq!(
        doc_b.attributes.get("text").and_then(|v| v.as_str()),
        Some("charlie patched unique")
    );
}

/// `branch_from_namespace` server-side copy: dest prefix has same object count as source.
#[tokio::test]
async fn s3_branch_from_namespace_clones_prefix() {
    let fixture = S3Fixture::from_testcontainers().await;
    let src = NAMESPACE_S3_BRANCH_SRC;
    let dest = NAMESPACE_S3_BRANCH_DEST;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    upsert_batch(
        &serve.base_url,
        src,
        json!([
            {
                "id": "branch-key-a",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "branch key parity alpha",
                    "tier": "pro"
                }
            },
            {
                "id": "branch-key-b",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "branch key parity bravo",
                    "tier": "free"
                }
            }
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, src, Duration::from_secs(60)).await;
    assert_index_objects(&fixture.client, &fixture.bucket, src).await;

    write_batch(
        &serve.base_url,
        dest,
        json!({"branch_from_namespace": src}),
    )
    .await;

    let src_prefix = format!("{ROOT_PREFIX}{src}/");
    let dest_prefix = format!("{ROOT_PREFIX}{dest}/");
    let src_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &src_prefix).await;
    let dest_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &dest_prefix).await;

    assert_eq!(
        src_keys.len(),
        dest_keys.len(),
        "branch must duplicate every source key (src={src_keys:?} dest={dest_keys:?})"
    );
    assert!(!src_keys.is_empty(), "source must have S3 objects before branch");

    for key in &src_keys {
        let suffix = key
            .strip_prefix(&src_prefix)
            .expect("source key under namespace prefix");
        let expected_dest = format!("{dest_prefix}{suffix}");
        assert!(
            dest_keys.contains(&expected_dest),
            "dest missing branch copy of {key} (expected {expected_dest})"
        );
    }

    let src_meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, src).await;
    let dest_meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, dest).await;
    assert_eq!(
        dest_meta.wal_commit_seq, src_meta.wal_commit_seq,
        "branch dest should inherit WAL commit seq from source"
    );
}

/// Base64 vector upsert (turbopuffer f32 LE) round-trips through `include_vectors` query options.
#[tokio::test]
async fn base64_vector_upsert_query_include_vectors_roundtrip() {
    use common::s3_harness::query_response_ns;

    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    let write_body = json!({
        "schema": { "embedding": "[3]f32" },
        "upsert_rows": [{
            "id": "b64-doc",
            "attributes": {
                "text": "base64 vector doc",
                "embedding": EMB_B64_THREE
            }
        }]
    });
    let resp = reqwest::Client::new()
        .post(format!("{}/v2/namespaces/{}", serve.base_url, NAMESPACE_VEC_B64))
        .json(&write_body)
        .send()
        .await
        .expect("write");
    assert_eq!(resp.status(), StatusCode::OK, "{}", resp.text().await.unwrap_or_default());

    let float_q = query_response_ns(
        &serve.base_url,
        NAMESPACE_VEC_B64,
        json!({
            "rank_by": ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            "top_k": 1,
            "include_vectors": true,
            "vector_encoding": "float"
        }),
    )
    .await;
    let row = &float_q["rows"][0];
    assert_eq!(row["id"], "b64-doc");
    assert_eq!(row["attributes"]["embedding"], json!([1.0, 0.0, 0.0]));

    let b64_q = query_response_ns(
        &serve.base_url,
        NAMESPACE_VEC_B64,
        json!({
            "rank_by": ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            "top_k": 1,
            "include_vectors": ["embedding"],
            "vector_encoding": "base64"
        }),
    )
    .await;
    let emb = b64_q["rows"][0]["attributes"]["embedding"]
        .as_str()
        .expect("base64 embedding");
    assert_eq!(emb, EMB_B64_THREE);
}

/// Two serve processes on one MinIO bucket: B cold-starts from S3 only; meta ETag and WAL unchanged.
#[tokio::test]
async fn s3_two_instances_share_bucket() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_S3_TWO_INST;

    let mut serve_a = ServeHandle::spawn(&fixture, &listen);
    serve_a.wait_ready().await;

    upsert_batch(
        &serve_a.base_url,
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
    wait_until_indexed(&serve_a.base_url, ns, Duration::from_secs(30)).await;

    let meta_key = openpuffer::meta::meta_key(ns);
    let etag_before = head_object_etag(&fixture.client, &fixture.bucket, &meta_key).await;
    let wal_prefix = format!("{ROOT_PREFIX}{ns}/wal/");
    let wal_keys_before = list_keys_with_prefix(&fixture.client, &fixture.bucket, &wal_prefix).await;
    assert!(
        wal_keys_before.iter().any(|k| k.ends_with("00000001.bin")),
        "expected wal/00000001.bin before restart, keys={wal_keys_before:?}"
    );

    serve_a.stop();
    drop(serve_a);
    sleep(Duration::from_millis(500)).await;

    let serve_b = ServeHandle::spawn_with_cache(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
    );
    serve_b.wait_ready().await;

    let vector_ids = query_ids_ns(
        &serve_b.base_url,
        ns,
        json!(["vector", "ANN", "embedding", [1.0, 0.0, 0.0]]),
        None,
    )
    .await;
    assert_eq!(
        vector_ids.first().map(String::as_str),
        Some("doc-a"),
        "instance B cold query from S3, got {vector_ids:?}"
    );

    let fts_ids = query_ids_ns(
        &serve_b.base_url,
        ns,
        json!(["BM25", "text", "alpha"]),
        None,
    )
    .await;
    assert!(
        fts_ids.contains(&"doc-a".to_string()) && fts_ids.contains(&"doc-c".to_string()),
        "FTS via pure S3 on instance B, got {fts_ids:?}"
    );

    let etag_after = head_object_etag(&fixture.client, &fixture.bucket, &meta_key).await;
    assert_eq!(
        etag_before, etag_after,
        "meta.json ETag must be unchanged across serve restart (read-only on B)"
    );

    let wal_keys_after = list_keys_with_prefix(&fixture.client, &fixture.bucket, &wal_prefix).await;
    assert!(
        wal_keys_after.iter().any(|k| k.ends_with("00000001.bin")),
        "wal segments must remain on S3, keys={wal_keys_after:?}"
    );
    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert!(meta.wal_commit_seq >= 1, "meta must record WAL commits");
}

/// Cold query (`--cache-dir=""`) reports batched S3 roundtrips and loads index from MinIO.
#[tokio::test]
async fn s3_cold_query_reports_roundtrips_on_minio() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_S3_COLD_RT;

    let serve = ServeHandle::spawn_with_cache(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
    );
    serve.wait_ready().await;

    upsert_batch(
        &serve.base_url,
        ns,
        json!([
            {
                "id": "cold-a",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "cold roundtrip alpha",
                    "tier": "pro"
                }
            },
            {
                "id": "cold-b",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "cold roundtrip bravo",
                    "tier": "free"
                }
            }
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    let l0_key = format!("{ROOT_PREFIX}{ns}/index/embedding/centroids-l0.bin");
    assert_key_exists(&fixture.client, &fixture.bucket, &l0_key).await;

    let client = reqwest::Client::new();
    let reset = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");
    assert_eq!(reset.status(), StatusCode::OK);

    let resp = client
        .post(format!("{}/v2/namespaces/{ns}/query", serve.base_url))
        .json(&json!({
            "rank_by": ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            "top_k": 2
        }))
        .send()
        .await
        .expect("cold query");
    assert_eq!(resp.status(), StatusCode::OK, "cold query failed");
    let roundtrips_hdr = resp
        .headers()
        .get("x-openpuffer-storage-roundtrips")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u32>().ok());
    let body: Value = resp.json().await.expect("query json");
    let roundtrips_json = body["performance"]["storage_roundtrips"]
        .as_u64()
        .expect("performance.storage_roundtrips");
    assert!(
        roundtrips_json >= 2,
        "cold batched load should report >=2 storage roundtrips, got {roundtrips_json}"
    );
    if let Some(hdr) = roundtrips_hdr {
        assert!(
            hdr >= 2,
            "X-Openpuffer-Storage-Roundtrips header should be >=2, got {hdr}"
        );
    }

    let stats: Value = client
        .get(format!("{}/v1/debug/cache-stats", serve.base_url))
        .send()
        .await
        .expect("cache stats")
        .json()
        .await
        .expect("stats json");
    // Cold path uses s3_batch parallel GetObject, not segment cache counter.
    let _ = stats["s3_get_count"].as_u64();

    assert_key_exists(&fixture.client, &fixture.bucket, &l0_key).await;
    assert_two_level_centroids_on_backend(&fixture.client, &fixture.bucket, ns).await;
}

/// WAL compaction on MinIO: old segment deleted, snapshot.bin present, decode + query still correct.
#[tokio::test]
async fn s3_compaction_removes_old_wal_objects() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_S3_COMPACT;

    let serve = ServeHandle::spawn_with_options(
        &fixture,
        &listen,
        None,
        Some(1),
        Some(50),
    );
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        ns,
        json!({
            "schema": {
                "text": { "type": "string", "full_text_search": true },
                "embedding": "[3]f32"
            },
            "upsert_rows": [{
                "id": "s3c-0",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "s3 compact unique zero"
                }
            }]
        }),
    )
    .await;

    for i in 1..12 {
        upsert_batch(
            &serve.base_url,
            ns,
            json!([{
                "id": format!("s3c-{i}"),
                "attributes": {
                    "embedding": [0.1 * i as f64, 0.2, 0.3],
                    "text": format!("s3 compact unique term {i}")
                }
            }]),
        )
        .await;
    }

    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(90)).await;

    let snapshot_key = format!("{ROOT_PREFIX}{ns}/wal/snapshot.bin");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    let mut meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    loop {
        if meta.wal_snapshot_seq > 0
            && s3_object_exists(&fixture.client, &fixture.bucket, &snapshot_key).await
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "wal compaction did not finish, meta={meta:?}"
            );
        }
        sleep(Duration::from_millis(250)).await;
        meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    }

    assert!(
        meta.wal_commit_seq >= 12,
        "expected >=12 wal commits, meta={meta:?}"
    );
    assert_eq!(meta.index_cursor, meta.wal_commit_seq);

    let wal_prefix = format!("{ROOT_PREFIX}{ns}/wal/");
    let wal_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &wal_prefix).await;
    assert!(
        wal_keys.iter().any(|k| k == &snapshot_key),
        "expected wal/snapshot.bin on MinIO, keys={wal_keys:?}"
    );
    let first_wal = format!("{wal_prefix}00000001.bin");
    assert!(
        !s3_object_exists(&fixture.client, &fixture.bucket, &first_wal).await,
        "00000001.bin must be deleted after compaction"
    );

    let snap = decode_wal_snapshot_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert_eq!(snap.seq, meta.wal_snapshot_seq);
    let mut doc_ids: std::collections::HashSet<String> =
        snap.docs.iter().map(|d| d.id.clone()).collect();

    for key in &wal_keys {
        if !key.starts_with(&wal_prefix) || !key.ends_with(".bin") || key.ends_with("snapshot.bin")
        {
            continue;
        }
        let seq_str = key
            .strip_prefix(&wal_prefix)
            .and_then(|s| s.strip_suffix(".bin"))
            .expect("wal segment filename");
        let seq: u64 = seq_str.parse().expect("wal seq");
        let entry = decode_wal_entry_from_s3(&fixture.client, &fixture.bucket, ns, seq).await;
        for id in wal_upsert_ids(&entry) {
            doc_ids.insert(id);
        }
    }
    for i in 0..12 {
        assert!(
            doc_ids.contains(&format!("s3c-{i}")),
            "snapshot + tail WAL must cover s3c-{i}, have {doc_ids:?}"
        );
    }

    let serve_cold = ServeHandle::spawn_with_cache(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
    );
    serve_cold.wait_ready().await;

    let fts_ids = query_ids_ns(
        &serve_cold.base_url,
        ns,
        json!(["BM25", "text", "compact unique"]),
        None,
    )
    .await;
    assert!(
        fts_ids.iter().any(|id| id.starts_with("s3c-")),
        "query after compaction via cold S3 load, ids={fts_ids:?}"
    );
}

/// Three namespaces written concurrently; fair background indexer must not let one hot ns starve the others.
#[tokio::test]
async fn fair_multi_namespace_background_indexer() {
    let fixture = S3Fixture::from_testcontainers().await;

    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let serve = ServeHandle::spawn_with_options(
        &fixture,
        &listen,
        None,
        Some(FAIR_HOT_BATCH),
        None,
    );
    serve.wait_ready().await;

    let schema = json!({
        "text": {"type": "string", "full_text_search": true},
        "embedding": "[32]f32"
    });

    let base = serve.base_url.clone();
    let schema_hot = schema.clone();
    let schema_b = schema.clone();
    let hot = tokio::spawn({
        let base = base.clone();
        async move {
            for b in 0..FAIR_HOT_BATCHES {
                if b > 0 {
                    sleep(Duration::from_millis(1100)).await;
                }
                let start = b * FAIR_HOT_BATCH;
                let mut body = json!({
                    "upsert_columns": fair_upsert_columns(start, FAIR_HOT_BATCH, 32)
                });
                if b == 0 {
                    body["schema"] = schema_hot.clone();
                }
                write_batch(&base, NAMESPACE_FAIR_HOT, body).await;
            }
        }
    });

    let small_b = tokio::spawn({
        let base = base.clone();
        async move {
            write_batch(
                &base,
                NAMESPACE_FAIR_B,
                json!({
                    "schema": schema_b,
                    "upsert_rows": (0..12).map(|i| json!({
                        "id": format!("b-{i}"),
                        "attributes": {
                            "text": format!("fair namespace b doc {i}"),
                            "embedding": fair_embedding(i, 32)
                        }
                    })).collect::<Vec<_>>()
                }),
            )
            .await;
        }
    });

    let small_c = tokio::spawn({
        let base = base.clone();
        async move {
            write_batch(
                &base,
                NAMESPACE_FAIR_C,
                json!({
                    "schema": schema,
                    "upsert_rows": (0..12).map(|i| json!({
                        "id": format!("c-{i}"),
                        "attributes": {
                            "text": format!("fair namespace c doc {i}"),
                            "embedding": fair_embedding(i + 100, 32)
                        }
                    })).collect::<Vec<_>>()
                }),
            )
            .await;
        }
    });

    hot.await.expect("hot namespace writes");
    small_b.await.expect("fair-b writes");
    small_c.await.expect("fair-c writes");

    sleep(Duration::from_millis(1200)).await;

    let deadline = Duration::from_secs(60);
    let wait_all = async {
        wait_until_indexed(&base, NAMESPACE_FAIR_HOT, deadline).await;
        wait_until_indexed(&base, NAMESPACE_FAIR_B, deadline).await;
        wait_until_indexed(&base, NAMESPACE_FAIR_C, deadline).await;
    };
    tokio::time::timeout(deadline, wait_all)
        .await
        .expect("all three namespaces should index within 60s");

    let client = reqwest::Client::new();
    for ns in [NAMESPACE_FAIR_HOT, NAMESPACE_FAIR_B, NAMESPACE_FAIR_C] {
        let meta: Value = client
            .get(format!("{base}/v1/namespaces/{ns}"))
            .send()
            .await
            .expect("metadata")
            .json()
            .await
            .expect("metadata json");
        assert_eq!(
            meta["index_status"].as_str(),
            Some("up_to_date"),
            "{ns}: {meta}"
        );
        let cursor = meta["index_cursor"].as_u64().unwrap_or(0);
        let commit = meta["wal_commit_seq"].as_u64().unwrap_or(0);
        assert_eq!(cursor, commit, "{ns} index_cursor behind commit");
    }
}

fn fair_embedding(seed: usize, dim: usize) -> Value {
    let emb: Vec<f64> = (0..dim)
        .map(|d| ((seed * dim + d) as f64 * 0.001).sin())
        .collect();
    json!(emb)
}

/// FTS + filter index segments grow on MinIO across WAL batches; hybrid query hits indexed docs.
#[tokio::test]
async fn s3_fts_and_filter_segments_grow_on_minio() {
    let fixture = S3Fixture::from_testcontainers().await;
    let ns = NAMESPACE_S3_SEG_GROW;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    upsert_batch(
        &serve.base_url,
        ns,
        json!([{
            "id": "grow-1",
            "attributes": {
                "embedding": [1.0, 0.0, 0.0],
                "text": "segment growth alpha batch one",
                "tier": "a"
            }
        }]),
    )
    .await;
    sleep(Duration::from_millis(1500)).await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    let meta1 = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert_eq!(meta1.index_cursor, 1);
    let fts_id1 = *meta1.fts_segment_ids.last().expect("fts segment id");
    let filter_id1 = *meta1.filter_segment_ids.last().expect("filter segment id");
    let fts_key1 = format!("{ROOT_PREFIX}{ns}/index/fts-{fts_id1:08}.bin");
    let filter_key1 = format!("{ROOT_PREFIX}{ns}/index/filter-{filter_id1:08}.bin");
    let fts_size1 = object_size(&fixture.client, &fixture.bucket, &fts_key1).await;
    let filter_size1 = object_size(&fixture.client, &fixture.bucket, &filter_key1).await;
    assert!(fts_size1 > 0, "fts segment after batch 1 must be non-empty");
    assert!(filter_size1 > 0, "filter segment after batch 1 must be non-empty");

    upsert_batch(
        &serve.base_url,
        ns,
        json!([
            {
                "id": "grow-2",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "segment growth bravo batch two",
                    "tier": "b"
                }
            },
            {
                "id": "grow-3",
                "attributes": {
                    "embedding": [0.9, 0.1, 0.0],
                    "text": "segment growth alpha batch two extra",
                    "tier": "a"
                }
            }
        ]),
    )
    .await;
    sleep(Duration::from_millis(1500)).await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    let meta2 = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert_eq!(meta2.index_cursor, 2);
    let fts_id2 = *meta2.fts_segment_ids.last().expect("fts segment id after batch 2");
    let filter_id2 = *meta2
        .filter_segment_ids
        .last()
        .expect("filter segment id after batch 2");
    let fts_key2 = format!("{ROOT_PREFIX}{ns}/index/fts-{fts_id2:08}.bin");
    let filter_key2 = format!("{ROOT_PREFIX}{ns}/index/filter-{filter_id2:08}.bin");
    let fts_size2 = object_size(&fixture.client, &fixture.bucket, &fts_key2).await;
    let filter_size2 = object_size(&fixture.client, &fixture.bucket, &filter_key2).await;

    let fts_grew = meta2.fts_segment_ids.len() > meta1.fts_segment_ids.len()
        || fts_size2 > fts_size1
        || fts_id2 != fts_id1;
    let filter_grew = meta2.filter_segment_ids.len() > meta1.filter_segment_ids.len()
        || filter_size2 > filter_size1
        || filter_id2 != filter_id1;
    assert!(
        fts_grew,
        "FTS index must grow: meta1={meta1:?} size1={fts_size1} meta2={meta2:?} size2={fts_size2}"
    );
    assert!(
        filter_grew,
        "filter index must grow: meta1={meta1:?} size1={filter_size1} meta2={meta2:?} size2={filter_size2}"
    );

    let fts_bytes = get_object_bytes(&fixture.client, &fixture.bucket, &fts_key2).await;
    assert!(
        !fts_bytes.is_empty(),
        "GetObject on fts segment must return non-empty postings"
    );

    let hybrid = query_response_ns(
        &serve.base_url,
        ns,
        json!({
            "rank_by": [
                "Sum",
                ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
                ["BM25", "text", "alpha"]
            ],
            "top_k": 3
        }),
    )
    .await;
    let top = hybrid["rows"][0]["id"]
        .as_str()
        .expect("hybrid top hit id");
    assert!(
        top == "grow-1" || top == "grow-3",
        "hybrid top hit should be alpha+vector doc, got {top} rows={:?}",
        hybrid["rows"]
    );
    assert!(
        hybrid["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .any(|r| r["id"] == "grow-1"),
        "hybrid must return grow-1, got {:?}",
        hybrid["rows"]
    );
}

/// `copy_from_namespace` server-side copy: dest prefix has same object count as source.
#[tokio::test]
async fn s3_copy_from_namespace_duplicates_all_keys() {
    let fixture = S3Fixture::from_testcontainers().await;
    let src = NAMESPACE_S3_COPY_KEYS_SRC;
    let dest = NAMESPACE_S3_COPY_KEYS_DEST;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    upsert_batch(
        &serve.base_url,
        src,
        json!([
            {
                "id": "key-a",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "copy key parity alpha",
                    "tier": "pro"
                }
            },
            {
                "id": "key-b",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "copy key parity bravo",
                    "tier": "free"
                }
            }
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, src, Duration::from_secs(60)).await;
    assert_index_objects(&fixture.client, &fixture.bucket, src).await;

    write_batch(
        &serve.base_url,
        dest,
        json!({"copy_from_namespace": src}),
    )
    .await;

    let src_prefix = format!("{ROOT_PREFIX}{src}/");
    let dest_prefix = format!("{ROOT_PREFIX}{dest}/");
    let src_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &src_prefix).await;
    let dest_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &dest_prefix).await;

    assert_eq!(
        src_keys.len(),
        dest_keys.len(),
        "copy must duplicate every source key (src={src_keys:?} dest={dest_keys:?})"
    );
    assert!(
        !src_keys.is_empty(),
        "source namespace must have S3 objects before copy"
    );

    for key in &src_keys {
        let suffix = key
            .strip_prefix(&src_prefix)
            .expect("source key under namespace prefix");
        let expected_dest = format!("{dest_prefix}{suffix}");
        assert!(
            dest_keys.contains(&expected_dest),
            "dest missing copy of {key} (expected {expected_dest})"
        );
    }

    let dest_meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, dest).await;
    assert!(
        dest_meta.fts_segment_ids.len() >= 1 && dest_meta.filter_segment_ids.len() >= 1,
        "copied meta should retain index segment chains: {dest_meta:?}"
    );
}

/// WAL compaction + warm + eventual query: indexed docs visible, zero S3 GETs, tail doc hidden.
#[tokio::test]
async fn wal_compaction_warm_eventual_query_cross_feature() {
    let fixture = S3Fixture::from_testcontainers().await;
    let cache_dir = tempfile::tempdir().expect("cache tempdir");
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn_with_options(
        &fixture,
        &listen,
        Some(cache_dir.path().to_path_buf()),
        Some(1),
        Some(50),
    );
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_BB3_COMPACT_EV,
        json!({
            "schema": {
                "text": { "type": "string", "full_text_search": true },
                "embedding": "[3]f32"
            },
            "upsert_rows": [{
                "id": "compact-ev-0",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "compaction eventual warm unique zero"
                }
            }]
        }),
    )
    .await;

    for i in 1..15 {
        upsert_batch(
            &serve.base_url,
            NAMESPACE_BB3_COMPACT_EV,
            json!([{
                "id": format!("compact-ev-{i}"),
                "attributes": {
                    "embedding": [0.1 * i as f64, 0.2, 0.3],
                    "text": format!("compaction eventual warm unique term {i}")
                }
            }]),
        )
        .await;
    }

    wait_until_indexed(
        &serve.base_url,
        NAMESPACE_BB3_COMPACT_EV,
        Duration::from_secs(90),
    )
    .await;

    let snapshot_key = format!("{ROOT_PREFIX}{NAMESPACE_BB3_COMPACT_EV}/wal/snapshot.bin");
    let wal_prefix = format!("{ROOT_PREFIX}{NAMESPACE_BB3_COMPACT_EV}/wal/");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_BB3_COMPACT_EV).await;
        let wal_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &wal_prefix).await;
        let segment_wals: Vec<_> = wal_keys
            .iter()
            .filter(|k| k.ends_with(".bin") && !k.ends_with("snapshot.bin"))
            .collect();
        if meta.wal_snapshot_seq > 0
            && s3_object_exists(&fixture.client, &fixture.bucket, &snapshot_key).await
            && segment_wals.len() <= 3
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("wal compaction did not finish: meta={meta:?}");
        }
        sleep(Duration::from_millis(250)).await;
    }

    let client = reqwest::Client::new();
    let warm_resp = client
        .post(format!(
            "{}/v1/namespaces/{NAMESPACE_BB3_COMPACT_EV}/warm",
            serve.base_url
        ))
        .send()
        .await
        .expect("warm");
    assert_eq!(warm_resp.status(), StatusCode::OK);

    upsert_batch(
        &serve.base_url,
        NAMESPACE_BB3_COMPACT_EV,
        json!([{
            "id": "compact-ev-unindexed",
            "attributes": {
                "embedding": [0.0, 1.0, 0.0],
                "text": "compaction eventual unindexed tail only"
            }
        }]),
    )
    .await;

    let reset = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");
    assert_eq!(reset.status(), StatusCode::OK);

    let resp = query_response_ns(
        &serve.base_url,
        NAMESPACE_BB3_COMPACT_EV,
        json!({
            "rank_by": ["BM25", "text", "compaction eventual warm"],
            "top_k": 5,
            "consistency": "eventual"
        }),
    )
    .await;
    let ids: Vec<String> = resp["rows"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        ids.iter().any(|id| id.starts_with("compact-ev-") && id != "compact-ev-unindexed"),
        "eventual after compaction+warm must return indexed docs, got {ids:?}"
    );
    assert!(
        !ids.contains(&"compact-ev-unindexed".to_string()),
        "eventual must not see unindexed tail doc, got {ids:?}"
    );

    let stats_resp = client
        .get(format!("{}/v1/debug/cache-stats", serve.base_url))
        .send()
        .await
        .expect("stats");
    let stats: Value = stats_resp.json().await.expect("stats json");
    assert_eq!(
        stats["s3_get_count"].as_u64(),
        Some(0),
        "compaction + warm + eventual query should not S3 GetObject"
    );
}

/// branch_from_namespace + patch_by_filter: branch patches via filter index; source unchanged.
#[tokio::test]
async fn branch_patch_by_filter_cross_feature() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_BB3_BRANCH_SRC,
        json!({
            "schema": {
                "text": { "type": "string", "full_text_search": true },
                "tier": { "type": "string", "filterable": true },
                "embedding": "[3]f32"
            },
            "upsert_rows": [
                {
                    "id": "branch-a",
                    "attributes": {
                        "embedding": [1.0, 0.0, 0.0],
                        "text": "branch alpha source",
                        "tier": "pro"
                    }
                },
                {
                    "id": "branch-b",
                    "attributes": {
                        "embedding": [0.0, 1.0, 0.0],
                        "text": "branch bravo source",
                        "tier": "free"
                    }
                }
            ]
        }),
    )
    .await;
    wait_until_indexed(&serve.base_url, NAMESPACE_BB3_BRANCH_SRC, Duration::from_secs(60)).await;

    write_batch(
        &serve.base_url,
        NAMESPACE_BB3_BRANCH_DEST,
        json!({"branch_from_namespace": NAMESPACE_BB3_BRANCH_SRC}),
    )
    .await;

    let client = reqwest::Client::new();
    let patch_resp = client
        .post(format!(
            "{}/v2/namespaces/{}",
            serve.base_url, NAMESPACE_BB3_BRANCH_DEST
        ))
        .json(&json!({
            "patch_by_filter": {
                "filters": ["tier", "Eq", "free"],
                "patch": { "tier": "upgraded", "text": "branch bravo patched on dest" }
            }
        }))
        .send()
        .await
        .expect("patch_by_filter on branch");
    assert_eq!(patch_resp.status(), StatusCode::OK);
    let patch_body: Value = patch_resp.json().await.expect("patch json");
    assert_eq!(patch_body["rows_patched"].as_u64(), Some(1));

    sleep(Duration::from_millis(1500)).await;

    let branch_hits = query_ids_ns(
        &serve.base_url,
        NAMESPACE_BB3_BRANCH_DEST,
        json!(["BM25", "text", "patched"]),
        Some(json!(["tier", "Eq", "upgraded"])),
    )
    .await;
    assert!(
        branch_hits.contains(&"branch-b".to_string()),
        "branch should see patched doc-b, got {branch_hits:?}"
    );

    let src_free = query_ids_ns(
        &serve.base_url,
        NAMESPACE_BB3_BRANCH_SRC,
        json!(["BM25", "text", "bravo"]),
        Some(json!(["tier", "Eq", "free"])),
    )
    .await;
    assert!(
        src_free.contains(&"branch-b".to_string()),
        "source must still have free-tier branch-b, got {src_free:?}"
    );

    let src_upgraded = query_ids_ns(
        &serve.base_url,
        NAMESPACE_BB3_BRANCH_SRC,
        json!(["BM25", "text", "patched"]),
        None,
    )
    .await;
    assert!(
        src_upgraded.is_empty(),
        "source must not see branch patch text, got {src_upgraded:?}"
    );
}

/// [N]f16 schema + hybrid Sum rank_by returns doc matching both vector and BM25 signals.
#[tokio::test]
async fn f16_schema_hybrid_sum_query_cross_feature() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_BB3_F16_HYBRID,
        json!({
            "schema": {
                "text": { "type": "string", "full_text_search": true },
                "embedding": "[3]f16"
            },
            "upsert_rows": [
                {
                    "id": "f16-hybrid-a",
                    "attributes": {
                        "embedding": [1.0, 0.0, 0.0],
                        "text": "f16 hybrid alpha stressterm"
                    }
                },
                {
                    "id": "f16-hybrid-b",
                    "attributes": {
                        "embedding": [0.0, 1.0, 0.0],
                        "text": "f16 hybrid bravo other"
                    }
                }
            ]
        }),
    )
    .await;
    wait_until_indexed(&serve.base_url, NAMESPACE_BB3_F16_HYBRID, Duration::from_secs(60)).await;

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_BB3_F16_HYBRID).await;
    assert!(
        meta.schema.to_string().contains("f16"),
        "schema should record f16 vector: {}",
        meta.schema
    );

    let hybrid_ids = query_ids_ns(
        &serve.base_url,
        NAMESPACE_BB3_F16_HYBRID,
        json!([
            "Sum",
            ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            ["BM25", "text", "alpha stressterm"]
        ]),
        None,
    )
    .await;
    assert_eq!(
        hybrid_ids.first().map(String::as_str),
        Some("f16-hybrid-a"),
        "f16 hybrid Sum should rank alpha+vector doc first, got {hybrid_ids:?}"
    );
}

/// copy_from_namespace + hybrid query with filter on destination namespace.
#[tokio::test]
async fn copy_from_namespace_hybrid_filter_query_cross_feature() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_BB3_COPY_QUERY_SRC,
        json!({
            "schema": {
                "text": { "type": "string", "full_text_search": true },
                "tier": { "type": "string", "filterable": true },
                "embedding": "[3]f32"
            },
            "upsert_rows": [
                {
                    "id": "copy-hybrid-a",
                    "attributes": {
                        "embedding": [1.0, 0.0, 0.0],
                        "text": "copy hybrid alpha stressterm",
                        "tier": "pro"
                    }
                },
                {
                    "id": "copy-hybrid-b",
                    "attributes": {
                        "embedding": [0.9, 0.1, 0.0],
                        "text": "copy hybrid alpha stressterm",
                        "tier": "free"
                    }
                }
            ]
        }),
    )
    .await;
    wait_until_indexed(
        &serve.base_url,
        NAMESPACE_BB3_COPY_QUERY_SRC,
        Duration::from_secs(60),
    )
    .await;

    write_batch(
        &serve.base_url,
        NAMESPACE_BB3_COPY_QUERY_DEST,
        json!({"copy_from_namespace": NAMESPACE_BB3_COPY_QUERY_SRC}),
    )
    .await;

    let hybrid_pro = query_ids_ns(
        &serve.base_url,
        NAMESPACE_BB3_COPY_QUERY_DEST,
        json!([
            "Sum",
            ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            ["BM25", "text", "alpha stressterm"]
        ]),
        Some(json!(["tier", "Eq", "pro"])),
    )
    .await;
    assert_eq!(
        hybrid_pro.first().map(String::as_str),
        Some("copy-hybrid-a"),
        "copied dest hybrid+filter should return pro-tier top hit, got {hybrid_pro:?}"
    );
    assert!(
        !hybrid_pro.contains(&"copy-hybrid-b".to_string()),
        "filter tier=pro must exclude free-tier doc on dest, got {hybrid_pro:?}"
    );
}

/// Two vector columns: separate `index/{field}/centroids-l0.bin` on MinIO; ANN per field.
#[tokio::test]
async fn s3_two_vector_fields_separate_index_paths() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_S3_TWO_VEC,
        json!({
            "schema": {
                "embedding_a": "[4]f32",
                "embedding_b": "[4]f32"
            },
            "upsert_rows": [
                {
                    "id": "doc-a",
                    "attributes": {
                        "embedding_a": [1.0, 0.0, 0.0, 0.0],
                        "embedding_b": [0.0, 1.0, 0.0, 0.0]
                    }
                },
                {
                    "id": "doc-b",
                    "attributes": {
                        "embedding_a": [0.0, 1.0, 0.0, 0.0],
                        "embedding_b": [1.0, 0.0, 0.0, 0.0]
                    }
                }
            ]
        }),
    )
    .await;
    wait_until_indexed(
        &serve.base_url,
        NAMESPACE_S3_TWO_VEC,
        Duration::from_secs(60),
    )
    .await;

    let meta =
        fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_S3_TWO_VEC).await;
    assert_meta_vector_fields(
        &meta,
        &[("embedding_a", 4), ("embedding_b", 4)],
    );
    assert_eq!(meta.vector_field.as_str(), "embedding_a");
    assert_eq!(meta.dimensions, 4);

    assert_centroids_l0_for_field(
        &fixture.client,
        &fixture.bucket,
        NAMESPACE_S3_TWO_VEC,
        "embedding_a",
    )
    .await;
    assert_centroids_l0_for_field(
        &fixture.client,
        &fixture.bucket,
        NAMESPACE_S3_TWO_VEC,
        "embedding_b",
    )
    .await;

    let keys = list_namespace_keys(&fixture.client, &fixture.bucket, NAMESPACE_S3_TWO_VEC).await;
    let l0_a = centroids_l0_s3_key(NAMESPACE_S3_TWO_VEC, "embedding_a");
    let l0_b = centroids_l0_s3_key(NAMESPACE_S3_TWO_VEC, "embedding_b");
    assert!(
        keys.iter().any(|k| k == &l0_a),
        "S3 list must include {l0_a}, keys={keys:?}"
    );
    assert!(
        keys.iter().any(|k| k == &l0_b),
        "S3 list must include {l0_b}, keys={keys:?}"
    );
    assert!(
        !keys.iter().any(|k| {
            k.ends_with("centroids-l0.bin")
                && !k.contains("/index/embedding_a/")
                && !k.contains("/index/embedding_b/")
        }),
        "legacy flat centroids-l0 must not appear when two fields indexed, keys={keys:?}"
    );

    let ids_a = query_ids_ns(
        &serve.base_url,
        NAMESPACE_S3_TWO_VEC,
        json!(["vector", "ANN", "embedding_a", [1.0, 0.0, 0.0, 0.0]]),
        None,
    )
    .await;
    assert_eq!(
        ids_a.first().map(String::as_str),
        Some("doc-a"),
        "ANN on embedding_a should rank doc-a first, got {ids_a:?}"
    );

    let ids_b = query_ids_ns(
        &serve.base_url,
        NAMESPACE_S3_TWO_VEC,
        json!(["vector", "ANN", "embedding_b", [1.0, 0.0, 0.0, 0.0]]),
        None,
    )
    .await;
    assert_eq!(
        ids_b.first().map(String::as_str),
        Some("doc-b"),
        "ANN on embedding_b should rank doc-b first, got {ids_b:?}"
    );

    let bad_resp = reqwest::Client::new()
        .post(format!(
            "{}/v2/namespaces/{}",
            serve.base_url,
            namespace_path_segment(NAMESPACE_S3_TWO_VEC)
        ))
        .json(&json!({ "schema": { "embedding_c": "[4]f32" } }))
        .send()
        .await
        .expect("schema write");
    assert_eq!(
        bad_resp.status(),
        StatusCode::BAD_REQUEST,
        "third vector field in schema must be rejected: {}",
        bad_resp.text().await.unwrap_or_default()
    );
}

/// POST query and return HTTP status + JSON body.
async fn query_expect(base_url: &str, namespace: &str, body: &str) -> (StatusCode, Value) {
    let resp = reqwest::Client::new()
        .post(format!(
            "{base_url}/v2/namespaces/{}/query",
            namespace_path_segment(namespace)
        ))
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("query request");
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let parsed = serde_json::from_str(&text).unwrap_or_else(|_| json!({ "raw": text }));
    (status, parsed)
}

/// API errors use turbopuffer JSON shape on write, query, and validation failures.
#[tokio::test]
async fn api_error_shape_on_query_and_json_failures() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    let (status, body) = query_expect(
        &serve.base_url,
        "itest-query-errors-missing-ns",
        r#"{"rank_by": ["BM25", "text", "x"], "top_k": 1}"#,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "query on missing namespace: {body:?}"
    );
    assert_api_error_shape(&body);

    write_batch(
        &serve.base_url,
        "itest-query-errors-json",
        json!({ "upsert_rows": [{ "id": "e1", "attributes": { "text": "warm" } }] }),
    )
    .await;

    let (status, body) = query_expect(
        &serve.base_url,
        "itest-query-errors-json",
        r#"{"rank_by": ["BM25", "text", "warm"], "top_k": 1"#,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "malformed JSON: {body:?}"
    );
    assert_api_error_shape(&body);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .to_ascii_lowercase()
            .contains("json"),
        "expected JSON error message, got {body:?}"
    );

    let (status, body) = query_expect(
        &serve.base_url,
        "itest-query-errors-json",
        r#"{"rank_by": "not-an-array", "top_k": 1}"#,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "invalid rank_by: {body:?}"
    );
    assert_api_error_shape(&body);
}

/// Assert turbopuffer-style API error body: `{"error": "...", "status": "error"}`.
fn assert_api_error_shape(body: &Value) {
    assert_eq!(
        body["status"].as_str(),
        Some("error"),
        "expected status=error, got {body:?}"
    );
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "expected non-empty error message, got {body:?}"
    );
}

/// POST write and return HTTP status + JSON body (for limit violation tests).
///
/// Namespace names are percent-encoded in the path so invalid names like `bad/name`
/// reach the handler as one segment (otherwise `/` splits the route and returns 404).
async fn write_expect(base_url: &str, namespace: &str, body: Value) -> (StatusCode, Value) {
    let resp = reqwest::Client::new()
        .post(format!(
            "{base_url}/v2/namespaces/{}",
            namespace_path_segment(namespace)
        ))
        .json(&body)
        .send()
        .await
        .expect("write request");
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let parsed = serde_json::from_str(&text).unwrap_or_else(|_| json!({ "raw": text }));
    (status, parsed)
}

/// Server-side turbopuffer-style limits return 400 on violation.
#[tokio::test]
async fn server_limits_reject_invalid_namespace_and_batch_sizes() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn_with_limits(
        &fixture,
        &listen,
        None,
        None,
        Some(50),
        Some(2),
        Some(2),
    );
    serve.wait_ready().await;

    let (status, body) = write_expect(
        &serve.base_url,
        "bad/name",
        json!({ "upsert_rows": [{ "id": "a", "attributes": {} }] }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "invalid namespace: {body:?}");
    assert_api_error_shape(&body);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("namespace name"),
        "expected namespace error, got {body:?}"
    );

    let (status, body) = write_expect(
        &serve.base_url,
        "itest-limits-upsert",
        json!({
            "upsert_rows": [
                { "id": "u1", "attributes": {} },
                { "id": "u2", "attributes": {} },
                { "id": "u3", "attributes": {} }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "upsert batch: {body:?}");
    assert_api_error_shape(&body);
    assert!(
        body["error"].as_str().unwrap_or("").contains("maximum"),
        "expected upsert row limit error, got {body:?}"
    );

    write_batch(
        &serve.base_url,
        "itest-limits-filter",
        json!({
            "schema": {
                "tag": { "type": "string", "filterable": true },
                "text": { "type": "string", "full_text_search": true }
            },
            "upsert_rows": [
                { "id": "f1", "attributes": { "tag": "bulk", "text": "doc one" } },
                { "id": "f2", "attributes": { "tag": "bulk", "text": "doc two" } }
            ]
        }),
    )
    .await;
    write_batch(
        &serve.base_url,
        "itest-limits-filter",
        json!({
            "upsert_rows": [
                { "id": "f3", "attributes": { "tag": "bulk", "text": "doc three" } }
            ]
        }),
    )
    .await;
    sleep(Duration::from_millis(1200)).await;

    let (status, body) = write_expect(
        &serve.base_url,
        "itest-limits-filter",
        json!({ "delete_by_filter": ["tag", "Eq", "bulk"] }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "delete_by_filter cap: {body:?}");
    assert_api_error_shape(&body);
    assert!(
        body["error"].as_str().unwrap_or("").contains("filter matched"),
        "expected filter batch error, got {body:?}"
    );

    let (status, body) = write_expect(
        &serve.base_url,
        "itest-limits-filter",
        json!({
            "delete_by_filter": ["tag", "Eq", "bulk"],
            "delete_by_filter_allow_partial": true
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "partial delete: {body:?}");
    assert_eq!(body["rows_deleted"].as_u64(), Some(2));
    assert_eq!(body["rows_remaining"].as_bool(), Some(true));

    sleep(Duration::from_millis(1200)).await;

    let meta_resp = reqwest::Client::new()
        .get(format!(
            "{}/v1/namespaces/{}",
            serve.base_url, "itest-limits-filter"
        ))
        .send()
        .await
        .expect("metadata");
    assert_eq!(meta_resp.status(), StatusCode::OK);
    let meta: Value = meta_resp.json().await.expect("metadata json");
    assert_eq!(
        meta["approx_row_count"].as_u64(),
        Some(1),
        "one doc should remain after partial delete, meta={meta:?}"
    );
}

/// Single integration test exercising the full turbopuffer-style architecture on MinIO.
#[tokio::test]
async fn full_architecture_smoke() {
    let fixture = S3Fixture::from_testcontainers().await;
    let cache_dir = tempfile::tempdir().expect("cache tempdir");
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn_with_options(
        &fixture,
        &listen,
        Some(cache_dir.path().to_path_buf()),
        Some(1),
        Some(50),
    );
    serve.wait_ready().await;

    let ns = NAMESPACE_FULL_ARCH;
    let schema = json!({
        "text": {"type": "string", "full_text_search": true},
        "tier": {"type": "string", "filterable": true},
        "embedding_a": "[4]f32",
        "embedding_b": "[4]f32"
    });

    write_batch(
        &serve.base_url,
        ns,
        json!({
            "schema": schema,
            "upsert_columns": full_arch_upsert_columns(0, 4)
        }),
    )
    .await;

    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(90)).await;

    assert_wal_layout_after_write(&fixture.client, &fixture.bucket, ns).await;
    assert_index_objects(&fixture.client, &fixture.bucket, ns).await;
    assert_centroids_l0_for_field(&fixture.client, &fixture.bucket, ns, "embedding_a").await;
    assert_centroids_l0_for_field(&fixture.client, &fixture.bucket, ns, "embedding_b").await;

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert_meta_vector_fields(&meta, &[("embedding_a", 4), ("embedding_b", 4)]);
    assert_eq!(meta.schema["text"]["full_text_search"], json!(true));
    assert_eq!(meta.schema["tier"]["filterable"], json!(true));

    let client = reqwest::Client::new();
    let warm_resp = client
        .post(format!("{}/v1/namespaces/{ns}/warm", serve.base_url))
        .send()
        .await
        .expect("warm");
    assert_eq!(warm_resp.status(), StatusCode::OK, "warm failed");

    let strong = |body: Value| {
        let mut b = body;
        b["consistency"] = json!("strong");
        b
    };

    let vector_a = query_ids_ns(
        &serve.base_url,
        ns,
        json!(["vector", "ANN", "embedding_a", [1.0, 0.0, 0.0, 0.0]]),
        None,
    )
    .await;
    assert_eq!(
        vector_a.first().map(String::as_str),
        Some("arch-0"),
        "ANN embedding_a top-1, got {vector_a:?}"
    );

    let vector_b = query_ids_ns(
        &serve.base_url,
        ns,
        json!(["vector", "ANN", "embedding_b", [1.0, 0.0, 0.0, 0.0]]),
        None,
    )
    .await;
    assert_eq!(
        vector_b.first().map(String::as_str),
        Some("arch-3"),
        "ANN embedding_b top-1, got {vector_b:?}"
    );

    let fts_ids = query_ids_ns(
        &serve.base_url,
        ns,
        json!(["BM25", "text", "fullarch alpha"]),
        None,
    )
    .await;
    assert!(
        fts_ids.contains(&"arch-0".to_string()) && fts_ids.contains(&"arch-2".to_string()),
        "FTS alpha hits, got {fts_ids:?}"
    );

    let hybrid_ids = query_ids_ns(
        &serve.base_url,
        ns,
        json!([
            "Sum",
            ["vector", "ANN", "embedding_a", [1.0, 0.0, 0.0, 0.0]],
            ["BM25", "text", "fullarch alpha"]
        ]),
        None,
    )
    .await;
    assert!(
        hybrid_ids.contains(&"arch-0".to_string()),
        "hybrid should include arch-0, got {hybrid_ids:?}"
    );

    let filter_ids = query_response_ns(
        &serve.base_url,
        ns,
        strong(json!({
            "rank_by": ["vector", "ANN", "embedding_a", [1.0, 0.0, 0.0, 0.0]],
            "filters": ["tier", "Eq", "pro"],
            "top_k": 5
        })),
    )
    .await;
    let filter_row_ids: Vec<String> = filter_ids["rows"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        filter_row_ids.contains(&"arch-0".to_string())
            && filter_row_ids.contains(&"arch-2".to_string()),
        "strong filter tier=pro, got {filter_row_ids:?}"
    );
    assert!(
        !filter_row_ids.contains(&"arch-1".to_string()),
        "free-tier arch-1 must be excluded by filter, got {filter_row_ids:?}"
    );

    let patch_resp = client
        .post(format!(
            "{}/v2/namespaces/{}",
            serve.base_url,
            namespace_path_segment(ns)
        ))
        .json(&json!({
            "patch_by_filter": {
                "filters": ["tier", "Eq", "free"],
                "patch": { "tier": "upgraded", "text": "fullarch upgraded unique" }
            }
        }))
        .send()
        .await
        .expect("patch_by_filter");
    assert_eq!(patch_resp.status(), StatusCode::OK, "patch_by_filter failed");
    let patch_body: Value = patch_resp.json().await.expect("patch json");
    assert_eq!(
        patch_body["rows_patched"].as_u64(),
        Some(2),
        "two free-tier docs patched: {patch_body}"
    );
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(60)).await;

    write_batch(
        &serve.base_url,
        ns,
        json!({ "delete_by_filter": ["tier", "Eq", "upgraded"] }),
    )
    .await;
    sleep(Duration::from_millis(1200)).await;

    let remaining = query_ids_ns(
        &serve.base_url,
        ns,
        json!(["BM25", "text", "fullarch"]),
        None,
    )
    .await;
    assert!(
        remaining.contains(&"arch-0".to_string()) && remaining.contains(&"arch-2".to_string()),
        "pro-tier docs remain after partial delete_by_filter, got {remaining:?}"
    );
    assert!(
        !remaining.contains(&"arch-1".to_string()) && !remaining.contains(&"arch-3".to_string()),
        "upgraded docs must be deleted, got {remaining:?}"
    );

    let export_ids = export_all_ids(&serve.base_url, ns, None).await;
    assert_eq!(export_ids.len(), 2, "export should list 2 docs, got {export_ids:?}");
    assert!(export_ids.contains(&"arch-0".to_string()));
    assert!(export_ids.contains(&"arch-2".to_string()));

    for i in 4..12 {
        upsert_batch(
            &serve.base_url,
            ns,
            json!([{
                "id": format!("arch-wal-{i}"),
                "attributes": {
                    "embedding_a": [0.1 * i as f64, 0.2, 0.3, 0.4],
                    "embedding_b": [0.4, 0.3, 0.2, 0.1 * i as f64],
                    "text": format!("fullarch compaction wal filler term {i}"),
                    "tier": "pro"
                }
            }]),
        )
        .await;
    }

    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(120)).await;

    let snapshot_key = format!("{ROOT_PREFIX}{ns}/wal/snapshot.bin");
    let wal_prefix = format!("{ROOT_PREFIX}{ns}/wal/");
    let compact_deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
        let wal_keys = list_keys_with_prefix(&fixture.client, &fixture.bucket, &wal_prefix).await;
        let segment_wals: Vec<_> = wal_keys
            .iter()
            .filter(|k| k.ends_with(".bin") && !k.ends_with("snapshot.bin"))
            .collect();
        if meta.wal_snapshot_seq > 0
            && meta.index_cursor == meta.wal_commit_seq
            && s3_object_exists(&fixture.client, &fixture.bucket, &snapshot_key).await
            && segment_wals.len() <= 3
        {
            break;
        }
        if tokio::time::Instant::now() >= compact_deadline {
            panic!("wal compaction did not finish: meta={meta:?}");
        }
        sleep(Duration::from_millis(250)).await;
    }

    let warm2 = client
        .post(format!("{}/v1/namespaces/{ns}/warm", serve.base_url))
        .send()
        .await
        .expect("warm after compaction");
    assert_eq!(warm2.status(), StatusCode::OK);

    upsert_batch(
        &serve.base_url,
        ns,
        json!([{
            "id": "arch-unindexed-tail",
            "attributes": {
                "embedding_a": [0.0, 1.0, 0.0, 0.0],
                "embedding_b": [0.0, 0.0, 1.0, 0.0],
                "text": "fullarch unindexed tail only",
                "tier": "pro"
            }
        }]),
    )
    .await;

    let reset = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");
    assert_eq!(reset.status(), StatusCode::OK);

    let eventual_resp = query_response_ns(
        &serve.base_url,
        ns,
        json!({
            "rank_by": ["BM25", "text", "fullarch compaction"],
            "top_k": 10,
            "consistency": "eventual"
        }),
    )
    .await;
    let eventual_ids: Vec<String> = eventual_resp["rows"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        eventual_ids.iter().any(|id| id.starts_with("arch-wal-")),
        "eventual query must return indexed compaction docs, got {eventual_ids:?}"
    );
    assert!(
        !eventual_ids.contains(&"arch-unindexed-tail".to_string()),
        "eventual must not see unindexed tail, got {eventual_ids:?}"
    );

    let stats: Value = client
        .get(format!("{}/v1/debug/cache-stats", serve.base_url))
        .send()
        .await
        .expect("cache stats")
        .json()
        .await
        .expect("stats json");
    assert_eq!(
        stats["s3_get_count"].as_u64(),
        Some(0),
        "eventual query after warm must not S3 GetObject"
    );

    // Let background WAL compaction finish listing/copy races before branch clones prefix.
    sleep(Duration::from_secs(3)).await;

    write_batch(
        &serve.base_url,
        NAMESPACE_FULL_ARCH_BRANCH,
        json!({"branch_from_namespace": ns}),
    )
    .await;

    upsert_batch(
        &serve.base_url,
        NAMESPACE_FULL_ARCH_BRANCH,
        json!([{
            "id": "arch-branch-only",
            "attributes": {
                "embedding_a": [0.0, 1.0, 0.0, 0.0],
                "embedding_b": [1.0, 0.0, 0.0, 0.0],
                "text": "fullarch branch exclusive",
                "tier": "pro"
            }
        }]),
    )
    .await;
    wait_until_indexed(
        &serve.base_url,
        NAMESPACE_FULL_ARCH_BRANCH,
        Duration::from_secs(90),
    )
    .await;

    let branch_hits = query_ids_ns(
        &serve.base_url,
        NAMESPACE_FULL_ARCH_BRANCH,
        json!(["BM25", "text", "exclusive"]),
        None,
    )
    .await;
    assert!(
        branch_hits.contains(&"arch-branch-only".to_string()),
        "branch ns should see branch-only doc, got {branch_hits:?}"
    );

    let src_exclusive = query_ids_ns(
        &serve.base_url,
        ns,
        json!(["BM25", "text", "exclusive"]),
        None,
    )
    .await;
    assert!(
        !src_exclusive.contains(&"arch-branch-only".to_string()),
        "source must not see branch-only doc, got {src_exclusive:?}"
    );

    let branch_prefix = format!("{ROOT_PREFIX}{NAMESPACE_FULL_ARCH_BRANCH}/");
    let branch_keys =
        list_keys_with_prefix(&fixture.client, &fixture.bucket, &branch_prefix).await;
    assert!(
        branch_keys.iter().any(|k| k.contains("/wal/")),
        "branch namespace must have WAL on S3, keys={branch_keys:?}"
    );
}

fn full_arch_upsert_columns(start: usize, count: usize) -> Value {
    let mut ids = Vec::with_capacity(count);
    let mut texts = Vec::with_capacity(count);
    let mut tiers = Vec::with_capacity(count);
    let mut emb_a = Vec::with_capacity(count);
    let mut emb_b = Vec::with_capacity(count);
    for i in start..start + count {
        ids.push(json!(format!("arch-{i}")));
        let alpha = i % 2 == 0;
        texts.push(json!(if alpha {
            format!("fullarch alpha document {i}")
        } else {
            format!("fullarch bravo document {i}")
        }));
        tiers.push(json!(if i % 2 == 0 { "pro" } else { "free" }));
        emb_a.push(json!(if alpha {
            [1.0, 0.0, 0.0, 0.0]
        } else {
            [0.0, 1.0, 0.0, 0.0]
        }));
        // arch-3 is nearest to query [1,0,0,0] on embedding_b; arch-1 is farther.
        emb_b.push(json!(if i == 3 {
            [1.0, 0.0, 0.0, 0.0]
        } else if i == 1 {
            [0.5, 0.5, 0.0, 0.0]
        } else if alpha {
            [0.0, 1.0, 0.0, 0.0]
        } else {
            [0.0, 0.0, 1.0, 0.0]
        }));
    }
    json!({
        "id": ids,
        "text": texts,
        "tier": tiers,
        "embedding_a": emb_a,
        "embedding_b": emb_b
    })
}

fn fair_upsert_columns(start: usize, count: usize, dim: usize) -> Value {
    let mut ids = Vec::with_capacity(count);
    let mut texts = Vec::with_capacity(count);
    let mut embeddings = Vec::with_capacity(count);
    for i in start..start + count {
        ids.push(json!(format!("hot-{i}")));
        texts.push(json!(format!("fair hot stressterm document {i}")));
        embeddings.push(fair_embedding(i, dim));
    }
    json!({
        "id": ids,
        "text": texts,
        "embedding": embeddings
    })
}