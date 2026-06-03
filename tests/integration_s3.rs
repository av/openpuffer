//! S3 round-trip integration tests against MinIO via testcontainers.
//!
//! Asserts turbopuffer-style layout (`meta.json`, `wal/`, `index/`), background indexing,
//! vector / FTS / hybrid / filter queries, and restart persistence — no `docs/{id}.json`.

mod common;

use common::s3_harness::*;
use common::synthetic_workload::{
    assert_workload_filter_hybrid_counts, cold_query_protocol, filter_query_specs,
    hybrid_query_specs, load_manifest, load_queries, l1_workload_dir, recall_defaults,
    resolve_openpuffer_query, synthetic_128_schema, upsert_columns_batch,
};
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
const NAMESPACE_RECALL: &str = "itest-recall";
const NAMESPACE_RECALL_FILTER: &str = "itest-recall-filter";
const NAMESPACE_SYNTHETIC_128: &str = "itest-synthetic-128-g2";
const NAMESPACE_WAL_COMPACT: &str = "itest-wal-compact";
const NAMESPACE_UPSERT_COND: &str = "itest-upsert-cond";
const NAMESPACE_PATCH_COND: &str = "itest-patch-cond";
const NAMESPACE_DELETE_COND: &str = "itest-delete-cond";
const NAMESPACE_DATETIME_UPSERT_COND: &str = "itest-datetime-upsert-cond";
const NAMESPACE_ORDER_BY: &str = "itest-order-by";
const NAMESPACE_QUERY_BILLING: &str = "itest-query-billing";
const NAMESPACE_DISTANCE_METRIC: &str = "itest-distance-metric";
const NAMESPACE_AFFECTED_IDS: &str = "itest-affected-ids";
const NAMESPACE_S3_WAL_BYTES: &str = "itest-s3-wal-bytes";
const NAMESPACE_S3_L1_CENTROIDS: &str = "itest-s3-l1-centroids";
const NAMESPACE_VEC_B64: &str = "itest-vec-b64";
const NAMESPACE_INCLUDE_ATTRS: &str = "itest-include-attrs";
const NAMESPACE_FAIR_HOT: &str = "itest-fair-hot";
const NAMESPACE_FAIR_B: &str = "itest-fair-b";
const NAMESPACE_FAIR_C: &str = "itest-fair-c";
const FAIR_HOT_BATCH: usize = 400;
const FAIR_HOT_BATCHES: usize = 5;
const NAMESPACE_S3_TWO_INST: &str = "itest-s3-two-inst";
const NAMESPACE_MULTI_INST: &str = "itest-multi-inst-stateless";
const NAMESPACE_S3_COLD_RT: &str = "itest-s3-cold-roundtrips";
const NAMESPACE_COLD_WAL_TAIL: &str = "itest-cold-wal-tail-r4";
const NAMESPACE_COLD_EVENTUAL: &str = "itest-cold-eventual-wal";
const NAMESPACE_COLD_STRONG_RT: &str = "itest-cold-strong-rt-compare";
const NAMESPACE_COLD_EVENTUAL_RT: &str = "itest-cold-eventual-rt-compare";
const NAMESPACE_COLD_PROBE_BOUND: &str = "itest-cold-probe-bound";
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
const NAMESPACE_COLD_HYBRID_FILTER: &str = "itest-cold-hybrid-filter";
const NAMESPACE_COLD_PLAN_DEBUG: &str = "itest-cold-plan-debug";
const NAMESPACE_COLD_FTS_BM25: &str = "itest-cold-fts-bm25-filter";
const NAMESPACE_COLD_HYBRID_PRODUCT: &str = "itest-cold-hybrid-product";
const NAMESPACE_COLD_HYBRID_10K: &str = "itest-cold-hybrid-10k";
const NAMESPACE_COLD_TWO_VEC_10K: &str = "itest-cold-two-vector-fields-10k";
const NAMESPACE_COLD_INDEX_LAG_FILTER: &str = "itest-cold-index-lag-filter";
const NAMESPACE_COLD_EMPTY_DOCS: &str = "itest-cold-empty-docs";
const NAMESPACE_S3_V3_ANN: &str = "itest-s3-ann-v3";
const NAMESPACE_NONEXISTENT_COLD: &str = "itest-namespace-never-created-cold";
const NAMESPACE_FULL_ARCH: &str = "itest-full-arch";
const NAMESPACE_FULL_ARCH_BRANCH: &str = "itest-full-arch-branch";
const NAMESPACE_WAL_CORRUPT_FAIL: &str = "itest-wal-corrupt-fail";
const NAMESPACE_WAL_CORRUPT_SKIP: &str = "itest-wal-corrupt-skip";
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
    wait_until_indexed(&serve2.base_url, NAMESPACE, Duration::from_secs(45)).await;
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
    let dest_keys = list_keys_with_prefix_until(
        &fixture.client,
        &fixture.bucket,
        &dest_prefix,
        Duration::from_secs(45),
        |keys| {
            keys.iter().any(|k| k.contains("/wal/"))
                && keys.iter().any(|k| k.ends_with("meta.json"))
        },
    )
    .await;
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
    let dest_keys = list_keys_with_prefix_until(
        &fixture.client,
        &fixture.bucket,
        &dest_prefix,
        Duration::from_secs(45),
        |keys| keys.iter().any(|k| k.contains("/wal/")),
    )
    .await;
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
    assert!(
        meta["preferred_ann_version"].as_u64().is_some(),
        "namespace metadata must expose preferred_ann_version for large-tier gates"
    );

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

/// 10k upsert with filterable `tier`: ~5% `pro` (sparse subset for recall filter corpus checks).
/// 10k upsert with two vector columns (orthogonal embeddings per doc index).
fn stress_two_vector_upsert_columns(start: usize, count: usize) -> Value {
    let mut ids = Vec::with_capacity(count);
    let mut embeddings_a = Vec::with_capacity(count);
    let mut embeddings_b = Vec::with_capacity(count);
    for i in start..start + count {
        ids.push(json!(format!("doc-{i}")));
        let emb_a: Vec<f64> = (0..STRESS_DIM)
            .map(|d| ((i * STRESS_DIM + d) as f64 * 0.001).sin())
            .collect();
        let emb_b: Vec<f64> = (0..STRESS_DIM)
            .map(|d| ((i * STRESS_DIM + d) as f64 * 0.001).cos())
            .collect();
        embeddings_a.push(json!(emb_a));
        embeddings_b.push(json!(emb_b));
    }
    json!({
        "id": ids,
        "embedding_a": embeddings_a,
        "embedding_b": embeddings_b
    })
}

fn recall_filter_upsert_columns(start: usize, count: usize) -> Value {
    let mut ids = Vec::with_capacity(count);
    let mut texts = Vec::with_capacity(count);
    let mut tiers = Vec::with_capacity(count);
    let mut embeddings = Vec::with_capacity(count);
    for i in start..start + count {
        ids.push(json!(format!("doc-{i}")));
        texts.push(json!(format!("stressterm document number {i}")));
        tiers.push(json!(if i % 20 == 0 { "pro" } else { "free" }));
        let emb: Vec<f64> = (0..STRESS_DIM)
            .map(|d| ((i * STRESS_DIM + d) as f64 * 0.001).sin())
            .collect();
        embeddings.push(json!(emb));
    }
    json!({
        "id": ids,
        "text": texts,
        "tier": tiers,
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

    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(240)).await;
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
        test_started.elapsed() < Duration::from_secs(300),
        "test exceeded 300s wall clock"
    );
}

/// `POST /v1/namespaces/{name}/recall` returns turbopuffer recall metrics on indexed 10k namespace.
#[tokio::test]
async fn recall_http_response_shape_on_minio() {
    let test_started = std::time::Instant::now();
    let queries = load_queries(&l1_workload_dir());
    let (recall_num, recall_top_k) = recall_defaults(&queries);
    let fixture = S3Fixture::from_testcontainers().await;

    let cache_dir = tempfile::tempdir().expect("cache tempdir");
    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let mut serve = ServeHandle::spawn_with_options(
        &fixture,
        &listen,
        Some(cache_dir.path().to_path_buf()),
        Some(10_000),
        None,
    );
    serve.wait_ready().await;

    let ns = NAMESPACE_RECALL;
    let schema = json!({
        "text": {"type": "string", "full_text_search": true},
        "embedding": "[128]f32"
    });

    let batches = STRESS_DOCS / STRESS_BATCH;
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

    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(240)).await;

    let warm_resp = reqwest::Client::new()
        .post(format!("{}/v1/namespaces/{ns}/warm", serve.base_url))
        .send()
        .await
        .expect("warm request");
    assert_eq!(warm_resp.status(), StatusCode::OK);

    let recall_resp = reqwest::Client::new()
        .post(format!("{}/v1/namespaces/{ns}/recall", serve.base_url))
        .json(&json!({ "num": recall_num, "top_k": recall_top_k }))
        .send()
        .await
        .expect("recall request");
    let recall_status = recall_resp.status();
    let recall_text = recall_resp.text().await.unwrap_or_default();
    assert_eq!(
        recall_status,
        StatusCode::OK,
        "recall failed: {recall_text}"
    );
    let body: Value = serde_json::from_str(&recall_text).expect("recall json");
    let avg_recall = body["avg_recall"].as_f64().expect("avg_recall number");
    let avg_ann = body["avg_ann_count"].as_f64().expect("avg_ann_count number");
    let avg_exhaustive = body["avg_exhaustive_count"]
        .as_f64()
        .expect("avg_exhaustive_count number");
    assert!(
        (0.0..=1.0).contains(&avg_recall),
        "avg_recall {avg_recall} out of range"
    );
    assert!(avg_ann > 0.0, "avg_ann_count {avg_ann}");
    assert!(avg_exhaustive > 0.0, "avg_exhaustive_count {avg_exhaustive}");
    assert!(
        avg_recall >= 0.85,
        "avg_recall {avg_recall} must be >= 0.85 on 10k indexed synthetic (num={recall_num}, top_k={recall_top_k})"
    );

    assert!(
        test_started.elapsed() < Duration::from_secs(300),
        "recall test exceeded 300s wall clock"
    );
    serve.stop();
}

/// G2 gate: ingest synthetic-128 column shape, `/recall` defaults from `queries.json`, all filter + hybrid queries, cold vector.
#[tokio::test]
async fn synthetic_128_g2_correctness_gates_on_minio() {
    let test_started = std::time::Instant::now();
    let workload_dir = l1_workload_dir();
    let manifest = load_manifest(&workload_dir);
    let queries = load_queries(&workload_dir);
    let dim = manifest["dim"].as_u64().expect("dim") as usize;
    let docs = STRESS_DOCS;
    let batch = STRESS_BATCH;

    let fixture = S3Fixture::from_testcontainers().await;
    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    // Empty `--cache-dir` disables segment cache so strong cold queries report `storage_roundtrips`
    // (matches bench_cold G2 gate and PLAN §4.2 cold protocol).
    let mut serve = ServeHandle::spawn_with_options(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
        Some(10_000),
        None,
    );
    serve.wait_ready().await;

    let ns = NAMESPACE_SYNTHETIC_128;
    let schema = synthetic_128_schema(dim);
    let batches = docs / batch;
    assert_eq!(batches * batch, docs);
    for b in 0..batches {
        if b > 0 {
            sleep(Duration::from_millis(1100)).await;
        }
        let start = b * batch;
        let mut body = json!({ "upsert_columns": upsert_columns_batch(start, batch, dim) });
        if b == 0 {
            body["schema"] = schema.clone();
        }
        write_batch(&serve.base_url, ns, body).await;
    }
    sleep(Duration::from_millis(1200)).await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(300)).await;

    let (recall_num, recall_top_k) = recall_defaults(&queries);
    let recall_resp = reqwest::Client::new()
        .post(format!("{}/v1/namespaces/{ns}/recall", serve.base_url))
        .json(&json!({ "num": recall_num, "top_k": recall_top_k }))
        .send()
        .await
        .expect("recall request");
    assert_eq!(recall_resp.status(), StatusCode::OK);
    let recall_body: Value = recall_resp.json().await.expect("recall json");
    let avg_recall = recall_body["avg_recall"].as_f64().expect("avg_recall");
    assert!(
        avg_recall >= 0.85,
        "synthetic-128 recall@{} (num={recall_num}) avg_recall {avg_recall} must be >= 0.85",
        recall_top_k
    );

    assert_workload_filter_hybrid_counts(&queries);
    let query_url = format!(
        "{}/v2/namespaces/{}/query",
        serve.base_url,
        namespace_path_segment(ns)
    );
    let client = reqwest::Client::new();

    for spec in filter_query_specs(&queries) {
        let name = spec["name"].as_str().unwrap_or("filter");
        let filter_query = resolve_openpuffer_query(
            spec.get("openpuffer_query").expect("filter openpuffer_query"),
            spec.get("vector").expect("filter vector"),
        );
        let filter_resp = client
            .post(&query_url)
            .json(&filter_query)
            .send()
            .await
            .unwrap_or_else(|e| panic!("filter query {name}: {e}"));
        assert_eq!(
            filter_resp.status(),
            StatusCode::OK,
            "filter query {name} failed"
        );
        let filter_body: Value = filter_resp.json().await.expect("filter json");
        let filter_rows = filter_body["rows"].as_array().expect("filter rows");
        assert!(
            !filter_rows.is_empty(),
            "filter query {name} must return rows"
        );
    }

    for spec in hybrid_query_specs(&queries) {
        let name = spec["name"].as_str().unwrap_or("hybrid");
        client
            .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
            .send()
            .await
            .expect("cache reset before hybrid");
        let hybrid_query = resolve_openpuffer_query(
            spec.get("openpuffer_query").expect("hybrid openpuffer_query"),
            spec.get("vector").expect("hybrid vector"),
        );
        let hybrid_resp = client
            .post(&query_url)
            .json(&hybrid_query)
            .send()
            .await
            .unwrap_or_else(|e| panic!("hybrid query {name}: {e}"));
        assert_eq!(
            hybrid_resp.status(),
            StatusCode::OK,
            "hybrid query {name} failed"
        );
        let hybrid_body: Value = hybrid_resp.json().await.expect("hybrid json");
        let hybrid_rows = hybrid_body["rows"].as_array().expect("hybrid rows");
        assert!(
            !hybrid_rows.is_empty(),
            "hybrid query {name} must return rows"
        );
        let roundtrips = hybrid_body["performance"]["storage_roundtrips"]
            .as_u64()
            .expect("hybrid storage_roundtrips");
        assert!(
            roundtrips <= 4,
            "hybrid query {name} storage_roundtrips {roundtrips} must be ≤ 4"
        );
    }

    let cold_proto = cold_query_protocol(&queries);
    let vector_spec = &queries["vector_queries"][0];
    let cold_query = resolve_openpuffer_query(
        vector_spec.get("openpuffer_query").expect("vector openpuffer_query"),
        vector_spec.get("vector").expect("vector"),
    );
    assert_eq!(cold_query["top_k"], cold_proto["top_k"]);
    assert_eq!(cold_query["consistency"], cold_proto["consistency"]);
    client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");
    let cold_resp = client
        .post(format!(
            "{}/v2/namespaces/{}/query",
            serve.base_url,
            namespace_path_segment(ns)
        ))
        .json(&cold_query)
        .send()
        .await
        .expect("cold vector query");
    assert_eq!(cold_resp.status(), StatusCode::OK);
    let cold_body: Value = cold_resp.json().await.expect("cold json");
    let roundtrips = cold_body["performance"]["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips");
    assert!(
        roundtrips <= 4,
        "synthetic-128 cold query storage_roundtrips {roundtrips} must be ≤ 4"
    );

    assert!(
        test_started.elapsed() < Duration::from_secs(360),
        "synthetic_128_g2 test exceeded 360s wall clock"
    );
    serve.stop();
}

/// `POST …/recall` with `filters` restricts ANN and brute to the same doc set (9b159bb).
#[tokio::test]
async fn recall_http_with_filters() {
    let test_started = std::time::Instant::now();
    let queries = load_queries(&l1_workload_dir());
    let (recall_num, recall_top_k) = recall_defaults(&queries);
    let fixture = S3Fixture::from_testcontainers().await;

    let cache_dir = tempfile::tempdir().expect("cache tempdir");
    let listen_port = free_port();
    let listen = format!("127.0.0.1:{listen_port}");
    let mut serve = ServeHandle::spawn_with_options(
        &fixture,
        &listen,
        Some(cache_dir.path().to_path_buf()),
        Some(10_000),
        None,
    );
    serve.wait_ready().await;

    let ns = NAMESPACE_RECALL_FILTER;
    let schema = json!({
        "text": {"type": "string", "full_text_search": true},
        "tier": {"type": "string", "filterable": true},
        "embedding": "[128]f32"
    });

    let batches = STRESS_DOCS / STRESS_BATCH;
    for b in 0..batches {
        if b > 0 {
            sleep(Duration::from_millis(1100)).await;
        }
        let start = b * STRESS_BATCH;
        let mut body = json!({ "upsert_columns": recall_filter_upsert_columns(start, STRESS_BATCH) });
        if b == 0 {
            body["schema"] = schema.clone();
        }
        write_batch(&serve.base_url, ns, body).await;
    }
    sleep(Duration::from_millis(1200)).await;

    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(240)).await;

    let warm_resp = reqwest::Client::new()
        .post(format!("{}/v1/namespaces/{ns}/warm", serve.base_url))
        .send()
        .await
        .expect("warm request");
    assert_eq!(warm_resp.status(), StatusCode::OK);

    let client = reqwest::Client::new();
    let recall_url = format!("{}/v1/namespaces/{ns}/recall", serve.base_url);
    let pro_filter = json!(["tier", "Eq", "pro"]);

    async fn post_recall(
        client: &reqwest::Client,
        url: &str,
        num: usize,
        top_k: usize,
        filters: Option<Value>,
    ) -> Value {
        let resp = client
            .post(url)
            .json(&json!({ "num": num, "top_k": top_k, "filters": filters }))
            .send()
            .await
            .expect("recall request");
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        assert_eq!(status, StatusCode::OK, "recall failed: {text}");
        serde_json::from_str(&text).expect("recall json")
    }

    let unfiltered = post_recall(
        &client,
        &recall_url,
        recall_num as usize,
        recall_top_k as usize,
        None,
    )
    .await;
    let filtered = post_recall(
        &client,
        &recall_url,
        recall_num as usize,
        recall_top_k as usize,
        Some(pro_filter.clone()),
    )
    .await;

    let unfiltered_recall = unfiltered["avg_recall"].as_f64().expect("avg_recall");
    let filtered_recall = filtered["avg_recall"].as_f64().expect("avg_recall");
    assert!(
        unfiltered_recall >= 0.85,
        "unfiltered avg_recall {unfiltered_recall} must be >= 0.85 (num={recall_num}, top_k={recall_top_k})"
    );
    assert!(
        filtered_recall >= 0.85,
        "filtered avg_recall {filtered_recall} must be >= 0.85 (ANN restricted to filter set, num={recall_num}, top_k={recall_top_k})"
    );

    // Large top_k: filtered corpus (~500 pro docs) caps brute/ANN pool vs full 10k.
    let unfiltered_wide = post_recall(&client, &recall_url, 10, 800, None).await;
    let filtered_wide = post_recall(
        &client,
        &recall_url,
        10,
        800,
        Some(pro_filter),
    )
    .await;
    let unfiltered_exhaustive = unfiltered_wide["avg_exhaustive_count"]
        .as_f64()
        .expect("avg_exhaustive_count");
    let filtered_exhaustive = filtered_wide["avg_exhaustive_count"]
        .as_f64()
        .expect("avg_exhaustive_count");
    assert!(
        filtered_exhaustive <= 520.0,
        "filtered exhaustive count {filtered_exhaustive} should reflect ~500-doc corpus"
    );
    assert!(
        unfiltered_exhaustive >= 790.0,
        "unfiltered exhaustive count {unfiltered_exhaustive} should reflect full 10k corpus at top_k=800"
    );
    assert!(
        filtered_exhaustive < unfiltered_exhaustive - 100.0,
        "filter must shrink evaluation corpus (filtered={filtered_exhaustive}, unfiltered={unfiltered_exhaustive})"
    );

    assert!(
        test_started.elapsed() < Duration::from_secs(300),
        "recall filter test exceeded 300s wall clock"
    );
    serve.stop();
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
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
                "wal compaction did not finish within 90s, meta={meta:?} wal_keys={wal_keys:?} segment_wals={segment_wals:?} missing_seqs={missing:?}"
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

    let client = reqwest::Client::new();
    let reset = client
        .post(format!(
            "{}/v1/debug/cache-stats/reset",
            serve2.base_url
        ))
        .send()
        .await
        .expect("cache reset");
    assert_eq!(reset.status(), StatusCode::OK);

    let cold_body = query_response_ns(
        &serve2.base_url,
        NAMESPACE_WAL_COMPACT,
        json!({
            "rank_by": ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            "top_k": 10,
            "consistency": "strong"
        }),
    )
    .await;
    let vector_ids: Vec<String> = cold_body["rows"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        vector_ids.first().map(String::as_str),
        Some("compact-0"),
        "cold ANN top-1 after compaction + restart, ids={vector_ids:?}"
    );

    let perf = cold_body["performance"]
        .as_object()
        .expect("performance after compaction cold query");
    let roundtrips = perf
        .get("storage_roundtrips")
        .and_then(|v| v.as_u64())
        .expect("storage_roundtrips on cold ANN query after compaction");
    assert!(
        roundtrips <= 4,
        "storage_roundtrips {roundtrips} must be ≤ 4 on caught-up cold query after compaction"
    );
    let probed = perf
        .get("ann_probed_clusters")
        .and_then(|v| v.as_u64())
        .expect("ann_probed_clusters on cold ANN query after compaction");
    assert!(
        probed >= 1,
        "cold ANN after compaction must report probed clusters, got {probed}"
    );
    let cold_keys = perf
        .get("cold_s3_keys_fetched")
        .and_then(|v| v.as_u64())
        .expect("cold_s3_keys_fetched on cold ANN query after compaction");
    assert!(
        cold_keys >= 1,
        "cold query after compaction must fetch S3 keys, got {cold_keys}"
    );

    let recall_resp = client
        .post(format!(
            "{}/v1/namespaces/{}/recall",
            serve2.base_url,
            NAMESPACE_WAL_COMPACT
        ))
        .json(&json!({ "num": 5, "top_k": 10 }))
        .send()
        .await
        .expect("recall after compaction cold restart");
    let recall_status = recall_resp.status();
    let recall_text = recall_resp.text().await.unwrap_or_default();
    assert_eq!(
        recall_status,
        StatusCode::OK,
        "recall after compaction failed: {recall_text}"
    );
    let recall_body: Value = serde_json::from_str(&recall_text).expect("recall json");
    let avg_recall = recall_body["avg_recall"].as_f64().expect("avg_recall");
    assert!(
        avg_recall >= 0.5,
        "recall@10 after compaction cold path {avg_recall} must be ≥ 0.5 on 15-doc fixture"
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

/// `delete_condition`: delete only when condition passes; missing ids ignored; `$ref_new` is null.
#[tokio::test]
async fn delete_condition_deletes_matching_docs_only() {
    let fixture = S3Fixture::from_testcontainers().await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_DELETE_COND,
        json!({
            "upsert_rows": [
                { "id": "active-1", "attributes": { "status": "active" } },
                { "id": "inactive-1", "attributes": { "status": "inactive" } }
            ]
        }),
    )
    .await;
    sleep(Duration::from_millis(1200)).await;

    let http = reqwest::Client::new();
    let write_url = format!(
        "{}/v2/namespaces/{NAMESPACE_DELETE_COND}",
        serve.base_url
    );
    let resp = http
        .post(&write_url)
        .json(&json!({
            "delete_condition": ["status", "Eq", "active"],
            "deletes": ["active-1", "inactive-1", "missing-1"]
        }))
        .send()
        .await
        .expect("conditional delete");
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.expect("write json");
    assert_eq!(body["rows_deleted"].as_u64(), Some(1), "body={body}");
    assert_eq!(body["rows_affected"].as_u64(), Some(1));

    sleep(Duration::from_millis(1200)).await;

    let export = http
        .get(format!(
            "{}/v1/namespaces/{NAMESPACE_DELETE_COND}/export",
            serve.base_url
        ))
        .send()
        .await
        .expect("export");
    assert_eq!(export.status(), StatusCode::OK);
    let exported: Value = export.json().await.expect("export json");
    let rows = exported["rows"].as_array().expect("export rows");
    let ids: Vec<&str> = rows
        .iter()
        .map(|row| row["id"].as_str().expect("id"))
        .collect();
    assert!(
        ids.contains(&"inactive-1"),
        "inactive doc must remain, ids={ids:?}"
    );
    assert!(
        !ids.contains(&"active-1"),
        "active doc must be deleted, ids={ids:?}"
    );
    assert!(
        !ids.contains(&"missing-1"),
        "missing id must not create a row, ids={ids:?}"
    );
}

/// `patch_condition`: patch only when condition passes; missing ids ignored.
#[tokio::test]
async fn patch_condition_patches_matching_docs_only() {
    let fixture = S3Fixture::from_testcontainers().await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_PATCH_COND,
        json!({
            "upsert_rows": [
                { "id": "active-1", "attributes": { "status": "active", "name": "before" } },
                { "id": "inactive-1", "attributes": { "status": "inactive", "name": "before" } }
            ]
        }),
    )
    .await;
    sleep(Duration::from_millis(1200)).await;

    let http = reqwest::Client::new();
    let write_url = format!(
        "{}/v2/namespaces/{NAMESPACE_PATCH_COND}",
        serve.base_url
    );
    let resp = http
        .post(&write_url)
        .json(&json!({
            "patch_condition": ["status", "Eq", "active"],
            "patch_rows": [
                { "id": "active-1", "attributes": { "name": "patched" } },
                { "id": "inactive-1", "attributes": { "name": "should-not-apply" } },
                { "id": "missing-1", "attributes": { "name": "no-doc" } }
            ]
        }))
        .send()
        .await
        .expect("conditional patch");
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.expect("write json");
    assert_eq!(body["rows_patched"].as_u64(), Some(1), "body={body}");
    assert_eq!(body["rows_affected"].as_u64(), Some(1));

    sleep(Duration::from_millis(1200)).await;

    let export = http
        .get(format!(
            "{}/v1/namespaces/{NAMESPACE_PATCH_COND}/export",
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
        names.get("active-1").map(String::as_str),
        Some("patched"),
        "active doc must be patched, names={names:?}"
    );
    assert_eq!(
        names.get("inactive-1").map(String::as_str),
        Some("before"),
        "inactive doc must be unchanged, names={names:?}"
    );
    assert!(
        !names.contains_key("missing-1"),
        "patch must not create missing doc, names={names:?}"
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

/// Query rows expose turbopuffer `$dist` (serde from `QueryRow::dist`) for BM25 and vector rank_by.
#[tokio::test]
async fn query_row_dist_present_for_bm25_and_vector() {
    const NS: &str = "itest-query-dist";
    let fixture = S3Fixture::from_testcontainers().await;

    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NS,
        json!({
            "upsert_rows": [
                {
                    "id": "qd-1",
                    "attributes": {
                        "text": "dist smoke alpha bravo",
                        "embedding": [1.0, 0.0, 0.0]
                    }
                },
                {
                    "id": "qd-2",
                    "attributes": {
                        "text": "dist smoke charlie delta",
                        "embedding": [0.0, 1.0, 0.0]
                    }
                }
            ]
        }),
    )
    .await;
    sleep(Duration::from_millis(1200)).await;

    let bm25 = query_response_ns(
        &serve.base_url,
        NS,
        json!({
            "rank_by": ["BM25", "text", "dist smoke alpha"],
            "top_k": 2
        }),
    )
    .await;
    assert_rows_have_numeric_dist(&bm25["rows"]);

    let vector = query_response_ns(
        &serve.base_url,
        NS,
        json!({
            "rank_by": ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            "top_k": 2
        }),
    )
    .await;
    assert_rows_have_numeric_dist(&vector["rows"]);
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
    assert!(!src_keys.is_empty(), "source must have S3 objects before branch");
    let dest_keys = list_keys_with_prefix_min_count(
        &fixture.client,
        &fixture.bucket,
        &dest_prefix,
        src_keys.len(),
        Duration::from_secs(45),
    )
    .await;

    assert_eq!(
        src_keys.len(),
        dest_keys.len(),
        "branch must duplicate every source key (src={src_keys:?} dest={dest_keys:?})"
    );

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

/// `include_attributes` as a field whitelist returns only the requested attribute keys.
#[tokio::test]
async fn include_attributes_field_projection() {
    use common::s3_harness::query_response_ns;

    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_INCLUDE_ATTRS,
        json!({
            "schema": { "embedding": "[3]f32" },
            "block_until_indexed": true,
            "upsert_rows": [{
                "id": "proj-1",
                "attributes": {
                    "text": "include attributes projection smoke",
                    "tier": "pro",
                    "secret": "hidden",
                    "embedding": [1.0, 0.0, 0.0]
                }
            }]
        }),
    )
    .await;

    let v = query_response_ns(
        &serve.base_url,
        NAMESPACE_INCLUDE_ATTRS,
        json!({
            "rank_by": ["BM25", "text", "projection smoke"],
            "top_k": 1,
            "include_attributes": ["text", "tier"]
        }),
    )
    .await;
    let attrs = v["rows"][0]["attributes"]
        .as_object()
        .expect("projected attributes");
    assert_eq!(attrs.get("text").and_then(Value::as_str), Some("include attributes projection smoke"));
    assert_eq!(attrs.get("tier").and_then(Value::as_str), Some("pro"));
    assert!(!attrs.contains_key("secret"));
    assert!(!attrs.contains_key("embedding"));

    let no_attrs = query_response_ns(
        &serve.base_url,
        NAMESPACE_INCLUDE_ATTRS,
        json!({
            "rank_by": ["BM25", "text", "projection smoke"],
            "top_k": 1,
            "include_attributes": false
        }),
    )
    .await;
    assert!(
        no_attrs["rows"][0]["attributes"].is_null(),
        "include_attributes:false must omit attributes"
    );
}

const NAMESPACE_BLOCK_INDEXED: &str = "itest-block-until-indexed";

/// `block_until_indexed: true` returns 200 only after `index_cursor` catches up.
#[tokio::test]
async fn block_until_indexed_write_waits_for_background_indexer() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn(&fixture, &listen);
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NAMESPACE_BLOCK_INDEXED,
        json!({
            "schema": { "text": { "type": "string", "full_text_search": true } },
            "block_until_indexed": true,
            "upsert_rows": [{
                "id": "block-1",
                "attributes": { "text": "block until indexed smoke" }
            }]
        }),
    )
    .await;

    let client = reqwest::Client::new();
    let meta_url = format!(
        "{}/v1/namespaces/{}",
        serve.base_url, NAMESPACE_BLOCK_INDEXED
    );
    let meta: Value = client
        .get(&meta_url)
        .send()
        .await
        .expect("metadata get")
        .json()
        .await
        .expect("metadata json");
    let cursor = meta["index_cursor"].as_u64().unwrap_or(0);
    let commit = meta["wal_commit_seq"].as_u64().unwrap_or(0);
    assert!(commit > 0, "wal_commit_seq should be set");
    assert_eq!(
        cursor, commit,
        "write with block_until_indexed must return after indexer caught up"
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
    let wal_keys_before = list_keys_with_prefix_until(
        &fixture.client,
        &fixture.bucket,
        &wal_prefix,
        Duration::from_secs(30),
        |keys| keys.iter().any(|k| k.ends_with("00000001.bin")),
    )
    .await;
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

/// Cold vector query over HTTP; returns status and error body text (no assert).
async fn cold_vector_query_http(
    base_url: &str,
    namespace: &str,
    rank_by: Value,
) -> (StatusCode, String) {
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/v2/namespaces/{}/query",
            base_url,
            namespace_path_segment(namespace)
        ))
        .json(&json!({ "rank_by": rank_by, "top_k": 5 }))
        .send()
        .await
        .expect("cold vector query request");
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    (status, body)
}

/// Two concurrent serve processes on one bucket: horizontal scale (not restart).
///
/// Indexer publishes `centroids-l0.bin` after L1/clusters so probed cold loads never observe a
/// new probe plan before segment objects exist (see `write_vector_index` in `indexer.rs`).
#[tokio::test]
async fn multi_instance_stateless_integration() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen_a = format!("127.0.0.1:{}", free_port());
    let listen_b = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_MULTI_INST;
    let cold_cache = Some(PathBuf::from(""));

    let serve_a = ServeHandle::spawn_with_cache(&fixture, &listen_a, cold_cache.clone());
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
    wait_until_indexed(&serve_a.base_url, ns, Duration::from_secs(45)).await;

    let meta_after_a = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert!(
        meta_after_a.wal_commit_seq >= 1,
        "meta must record WAL commits from instance A"
    );
    let wal_a = decode_wal_entry_from_s3(&fixture.client, &fixture.bucket, ns, 1).await;
    assert!(
        wal_a.upserts.iter().any(|u| u.id == "doc-a"),
        "WAL on S3 must contain doc-a from instance A"
    );
    assert_two_level_centroids_on_backend(&fixture.client, &fixture.bucket, ns).await;

    let serve_b = ServeHandle::spawn_with_cache(&fixture, &listen_b, cold_cache);
    serve_b.wait_ready().await;
    wait_until_indexed(&serve_b.base_url, ns, Duration::from_secs(45)).await;

    let vector_on_b = query_ids_ns(
        &serve_b.base_url,
        ns,
        json!(["vector", "ANN", "embedding", [1.0, 0.0, 0.0]]),
        None,
    )
    .await;
    assert_eq!(
        vector_on_b.first().map(String::as_str),
        Some("doc-a"),
        "instance B must read indexed data written by A, got {vector_on_b:?}"
    );

    let fts_on_b = query_ids_ns(
        &serve_b.base_url,
        ns,
        json!(["BM25", "text", "alpha"]),
        None,
    )
    .await;
    assert!(
        fts_on_b.contains(&"doc-a".to_string()) && fts_on_b.contains(&"doc-c".to_string()),
        "instance B FTS must see A's docs, got {fts_on_b:?}"
    );

    // While B's indexer rewrites vector segments on S3, A hammers probed cold queries (empty
    // cache on both instances). Must stay 200: L0 is published only after clusters exist.
    let rank_d = json!(["vector", "ANN", "embedding", [0.0, 0.0, 1.0]]);
    let rank_a = json!(["vector", "ANN", "embedding", [1.0, 0.0, 0.0]]);
    let url_a = serve_a.base_url.clone();
    let ns_hammer = ns.to_string();
    let hammer = tokio::spawn(async move {
        let mut failures = Vec::new();
        let mut handles = Vec::new();
        for i in 0..48 {
            let url = url_a.clone();
            let ns = ns_hammer.clone();
            let rank = if i % 2 == 0 {
                rank_a.clone()
            } else {
                rank_d.clone()
            };
            handles.push(tokio::spawn(async move {
                cold_vector_query_http(&url, &ns, rank).await
            }));
        }
        for h in handles {
            let (status, body) = h.await.expect("cold hammer join");
            if status != StatusCode::OK {
                failures.push((status, body));
            }
        }
        failures
    });
    upsert_batch(
        &serve_b.base_url,
        ns,
        json!([{
            "id": "doc-d",
            "attributes": {
                "embedding": [0.0, 0.0, 1.0],
                "text": "echo foxtrot instance b",
                "tier": "pro"
            }
        }]),
    )
    .await;
    let cold_failures = hammer.await.expect("cold hammer task");
    assert!(
        cold_failures.is_empty(),
        "cold queries during B index build must not fail (L0-last ordering); failures={cold_failures:?}"
    );
    wait_until_indexed(&serve_a.base_url, ns, Duration::from_secs(60)).await;

    let meta_after_b = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert!(
        meta_after_b.wal_commit_seq > meta_after_a.wal_commit_seq,
        "instance B write must advance wal_commit_seq on shared S3 meta"
    );
    let wal_b = decode_wal_entry_from_s3(
        &fixture.client,
        &fixture.bucket,
        ns,
        meta_after_b.wal_commit_seq,
    )
    .await;
    assert!(
        wal_b.upserts.iter().any(|u| u.id == "doc-d"),
        "latest WAL on S3 must contain doc-d from instance B"
    );

    let client = reqwest::Client::new();
    let warm_resp = client
        .post(format!("{}/v1/namespaces/{ns}/warm", serve_a.base_url))
        .send()
        .await
        .expect("warm on A");
    assert_eq!(warm_resp.status(), StatusCode::OK, "warm on A failed");

    let export_on_a = export_all_ids(&serve_a.base_url, ns, None).await;
    assert!(
        export_on_a.contains(&"doc-a".to_string()) && export_on_a.contains(&"doc-d".to_string()),
        "instance A after warm must see B's write via S3, got {export_on_a:?}"
    );

    let vector_on_a = query_ids_ns(
        &serve_a.base_url,
        ns,
        json!(["vector", "ANN", "embedding", [0.0, 0.0, 1.0]]),
        None,
    )
    .await;
    assert_eq!(
        vector_on_a.first().map(String::as_str),
        Some("doc-d"),
        "instance A query after warm must rank doc-d from B, got {vector_on_a:?}"
    );
}

/// Cold vector query fetches O(probe) cluster segments, not `num_fine_total` (10k indexed).
#[tokio::test]
async fn cold_vector_query_cluster_gets_bounded_by_probe_plan() {
    use openpuffer::index::vector::CentroidIndexL0;
    use openpuffer::s3_batch::{cluster_get_upper_bound, fetch_cold_vector_probed};

    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_COLD_PROBE_BOUND;

    let serve = ServeHandle::spawn_with_cache(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
    );
    serve.wait_ready().await;

    let schema = json!({
        "text": {"type": "string", "full_text_search": true},
        "embedding": "[128]f32"
    });
    let batches = STRESS_DOCS / STRESS_BATCH;
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
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(300)).await;

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert_eq!(
        meta.index_cursor, meta.wal_commit_seq,
        "namespace must be fully indexed"
    );

    let l0_key = format!("{ROOT_PREFIX}{ns}/index/embedding/centroids-l0.bin");
    let l0_bytes = get_object_bytes(&fixture.client, &fixture.bucket, &l0_key).await;
    let l0 = CentroidIndexL0::decode(&l0_bytes)
        .expect("decode centroids-l0")
        .clamp_probe_plan_for_query();
    assert!(
        l0.num_fine_total > 100,
        "fixture must have many fine clusters (got {})",
        l0.num_fine_total
    );

    let index_prefix = format!("{ROOT_PREFIX}{ns}/index/");
    let index_keys =
        list_keys_with_prefix(&fixture.client, &fixture.bucket, &index_prefix).await;
    let s3_cluster_objects = index_keys
        .iter()
        .filter(|k| k.contains("clusters-"))
        .count();
    assert!(
        s3_cluster_objects > 100,
        "S3 should list many cluster objects (got {s3_cluster_objects})"
    );
    assert!(
        s3_cluster_objects >= l0.num_fine_total as usize / 2,
        "cluster object count {s3_cluster_objects} should track num_fine_total {}",
        l0.num_fine_total
    );

    let query_vec: Vec<f64> = (0..STRESS_DIM)
        .map(|d| (d as f64 * 0.02).cos())
        .collect();
    let max_cluster_gets = cluster_get_upper_bound(&l0);

    let (vindex, probe_roundtrips, _probe_keys, _probed_clusters) = fetch_cold_vector_probed(
        &fixture.client,
        &fixture.bucket,
        ns,
        &meta,
        "embedding",
        l0.clone(),
        &query_vec,
    )
    .await
    .expect("probed cold vector fetch");
    let cluster_segments_fetched = vindex.clusters.len();
    assert!(
        cluster_segments_fetched <= max_cluster_gets,
        "probed cluster GETs {cluster_segments_fetched} must be ≤ probe bound {max_cluster_gets} \
         (probe_coarse={}, probe_fine={})",
        l0.probe_coarse,
        l0.probe_fine
    );
    assert!(
        cluster_segments_fetched < l0.num_fine_total as usize,
        "probed fetch {cluster_segments_fetched} must be << num_fine_total {}",
        l0.num_fine_total
    );
    assert!(
        cluster_segments_fetched * 10 < s3_cluster_objects,
        "probed fetch should not scale with full index: fetched {cluster_segments_fetched}, \
         s3_cluster_objects {s3_cluster_objects}"
    );
    assert!(
        probe_roundtrips <= 2,
        "L1+clusters share ≤2 logical roundtrips in fetch_cold_vector_probed, got {probe_roundtrips}"
    );

    let client = reqwest::Client::new();
    client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");
    let resp = client
        .post(format!(
            "{}/v2/namespaces/{}/query",
            serve.base_url,
            namespace_path_segment(ns)
        ))
        .json(&json!({
            "rank_by": ["vector", "ANN", "embedding", query_vec],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("cold HTTP query");
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.expect("query json");
    let perf = body["performance"].as_object().expect("performance");
    let ratio = perf["candidates_ratio"].as_f64().expect("candidates_ratio");
    assert!(
        ratio < 0.20,
        "cold ANN candidates_ratio {ratio} should stay sub-linear on 10k"
    );
    let roundtrips = perf["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips");
    assert!(
        roundtrips <= 4,
        "storage_roundtrips {roundtrips} must be ≤ 4 on caught-up strong cold query"
    );
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
    let perf = body["performance"].as_object().expect("performance");
    let roundtrips_json = perf["storage_roundtrips"]
        .as_u64()
        .expect("performance.storage_roundtrips");
    assert!(
        roundtrips_json >= 2,
        "cold batched load should report >=2 storage roundtrips, got {roundtrips_json}"
    );
    let cold_keys = perf["cold_s3_keys_fetched"]
        .as_u64()
        .expect("performance.cold_s3_keys_fetched");
    assert!(
        cold_keys >= 1,
        "cold vector query should report S3 keys fetched, got {cold_keys}"
    );
    let probed = perf["ann_probed_clusters"]
        .as_u64()
        .expect("performance.ann_probed_clusters");
    assert!(
        probed >= 1,
        "vector ANN cold query should report probed clusters, got {probed}"
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

/// Strong cold query with unindexed WAL tail: round-4 batched fetch, tail doc visible, metrics set.
#[tokio::test]
async fn cold_strong_unindexed_wal_tail_round4_on_minio() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_COLD_WAL_TAIL;

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
                "id": "indexed-a",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "indexed baseline alpha"
                }
            },
            {
                "id": "indexed-b",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "indexed baseline bravo"
                }
            }
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    upsert_batch(
        &serve.base_url,
        ns,
        json!([{
            "id": "tail-unindexed",
            "attributes": {
                "embedding": [0.99, 0.01, 0.0],
                "text": "written after index cursor"
            }
        }]),
    )
    .await;

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert!(
        meta.index_cursor < meta.wal_commit_seq,
        "expected index lag before background indexer catches up: cursor={} commit={}",
        meta.index_cursor,
        meta.wal_commit_seq
    );

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");

    let resp = client
        .post(format!("{}/v2/namespaces/{ns}/query", serve.base_url))
        .json(&json!({
            "rank_by": ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            "top_k": 3,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("cold strong query with wal tail");
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.expect("query json");
    let ids: Vec<String> = body["rows"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        ids.contains(&"tail-unindexed".to_string()),
        "strong cold query must see unindexed tail doc via exhaustive WAL scoring, got {ids:?}"
    );

    let perf = body["performance"].as_object().expect("performance");
    let roundtrips = perf["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips");
    assert!(
        roundtrips >= 3 && roundtrips <= 5,
        "cold strong query with index lag should use bootstrap + probe + optional WAL tail round, got {roundtrips}"
    );
    let cold_keys = perf["cold_s3_keys_fetched"]
        .as_u64()
        .expect("cold_s3_keys_fetched");
    assert!(
        cold_keys >= 2,
        "cold query with WAL tail should report multiple S3 keys fetched, got {cold_keys}"
    );
    let exhaustive = perf["exhaustive_search_count"]
        .as_u64()
        .expect("exhaustive_search_count");
    assert!(
        exhaustive >= 1,
        "unindexed tail doc should be scored exhaustively, got {exhaustive}"
    );

    let plan_keys =
        openpuffer::s3_batch::unindexed_wal_tail_keys(ns, &meta);
    assert!(
        !plan_keys.is_empty(),
        "planner round-4 keys should be non-empty while index lags commit"
    );
}

/// Cold + eventual consistency with index lag: skip WAL round 1 and round 4; indexed docs visible.
#[tokio::test]
async fn cold_eventual_unindexed_tail_skips_wal_on_minio() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_COLD_EVENTUAL;

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
                "id": "ev-indexed-a",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "eventual cold indexed alpha"
                }
            },
            {
                "id": "ev-indexed-b",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "eventual cold indexed bravo"
                }
            }
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    upsert_batch(
        &serve.base_url,
        ns,
        json!([{
            "id": "ev-tail-unindexed",
            "attributes": {
                "embedding": [0.99, 0.01, 0.0],
                "text": "eventual cold tail after index cursor"
            }
        }]),
    )
    .await;

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert!(
        meta.index_cursor < meta.wal_commit_seq,
        "index lag required: cursor={} commit={}",
        meta.index_cursor,
        meta.wal_commit_seq
    );

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");

    let resp = client
        .post(format!("{}/v2/namespaces/{ns}/query", serve.base_url))
        .json(&json!({
            "rank_by": ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            "top_k": 3,
            "consistency": "eventual"
        }))
        .send()
        .await
        .expect("cold eventual query");
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.expect("query json");
    let ids: Vec<String> = body["rows"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        ids.iter().any(|id| id.starts_with("ev-indexed-")),
        "eventual cold query must return indexed docs from probed clusters, got {ids:?}"
    );
    assert!(
        !ids.contains(&"ev-tail-unindexed".to_string()),
        "eventual cold must not see unindexed WAL tail doc, got {ids:?}"
    );

    let perf = body["performance"].as_object().expect("performance");
    let roundtrips = perf["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips");
    assert!(
        roundtrips >= 2 && roundtrips <= 4,
        "eventual cold: meta + bootstrap + probe (no WAL rounds), got {roundtrips}"
    );
    let exhaustive = perf["exhaustive_search_count"].as_u64().unwrap_or(0);
    assert_eq!(
        exhaustive, 0,
        "eventual cold must not exhaustively score unindexed tail, got {exhaustive}"
    );

    let plan = openpuffer::s3_batch::plan_cold_query(
        ns,
        &meta,
        &[("embedding".into(), vec![1.0, 0.0, 0.0])],
        &HashMap::new(),
        None,
        openpuffer::s3_batch::ColdPlanOpts {
            include_wal_round: false,
            include_wal_tail: false,
        },
    );
    assert!(plan.round1_keys.is_empty());
    assert!(plan.round4_keys.is_empty());
}

/// Strong cold with index lag uses more roundtrips than eventual on a fresh namespace (WAL round 1 + tail).
#[tokio::test]
async fn cold_strong_requires_more_roundtrips_than_eventual_with_tail_on_minio() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns_ev = NAMESPACE_COLD_EVENTUAL_RT;
    let ns_str = NAMESPACE_COLD_STRONG_RT;

    let serve = ServeHandle::spawn_with_cache(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
    );
    serve.wait_ready().await;

    for ns in [ns_ev, ns_str] {
        upsert_batch(
            &serve.base_url,
            ns,
            json!([
                {
                    "id": "cmp-indexed",
                    "attributes": {
                        "embedding": [1.0, 0.0, 0.0],
                        "text": "compare indexed"
                    }
                }
            ]),
        )
        .await;
        wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;
        upsert_batch(
            &serve.base_url,
            ns,
            json!([{
                "id": "cmp-tail",
                "attributes": {
                    "embedding": [0.99, 0.01, 0.0],
                    "text": "compare tail"
                }
            }]),
        )
        .await;
    }

    let mut index_lag = true;
    for ns in [ns_ev, ns_str] {
        let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
        if meta.index_cursor >= meta.wal_commit_seq {
            index_lag = false;
        }
    }

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");

    let eventual_body: Value = client
        .post(format!("{}/v2/namespaces/{ns_ev}/query", serve.base_url))
        .json(&json!({
            "rank_by": ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            "top_k": 2,
            "consistency": "eventual"
        }))
        .send()
        .await
        .expect("eventual query")
        .json()
        .await
        .expect("eventual json");
    let strong_body: Value = client
        .post(format!("{}/v2/namespaces/{ns_str}/query", serve.base_url))
        .json(&json!({
            "rank_by": ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            "top_k": 2,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("strong query")
        .json()
        .await
        .expect("strong json");

    let ev_rt = eventual_body["performance"]["storage_roundtrips"]
        .as_u64()
        .expect("eventual roundtrips");
    let st_rt = strong_body["performance"]["storage_roundtrips"]
        .as_u64()
        .expect("strong roundtrips");
    assert!(
        st_rt > ev_rt,
        "strong cold must use more roundtrips than eventual (strong={st_rt}, eventual={ev_rt})"
    );

    if index_lag {
        let strong_ids: Vec<String> = strong_body["rows"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .map(|r| r["id"].as_str().unwrap().to_string())
            .collect();
        let eventual_ids: Vec<String> = eventual_body["rows"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .map(|r| r["id"].as_str().unwrap().to_string())
            .collect();
        assert!(
            strong_ids.contains(&"cmp-tail".to_string()),
            "strong must see unindexed tail doc when index lags, got {strong_ids:?}"
        );
        assert!(
            !eventual_ids.contains(&"cmp-tail".to_string()),
            "eventual must not see tail doc when index lags, got {eventual_ids:?}"
        );
    }
}

/// Cold path: hybrid `Sum` (vector + BM25) with attribute filter returns only matching tier.
#[tokio::test]
async fn cold_hybrid_sum_vector_filter_on_minio() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_COLD_HYBRID_FILTER;

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
                "id": "cold-hybrid-a",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "cold hybrid alpha stressterm",
                    "tier": "pro"
                }
            },
            {
                "id": "cold-hybrid-b",
                "attributes": {
                    "embedding": [0.9, 0.1, 0.0],
                    "text": "cold hybrid alpha stressterm",
                    "tier": "free"
                }
            }
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await;

    let body = query_response_ns(
        &serve.base_url,
        ns,
        json!({
            "rank_by": [
                "Sum",
                ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
                ["BM25", "text", "alpha"]
            ],
            "filters": ["tier", "Eq", "pro"],
            "top_k": 3,
            "consistency": "strong"
        }),
    )
    .await;
    let ids: Vec<String> = body["rows"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        ids,
        vec!["cold-hybrid-a".to_string()],
        "hybrid+filter cold query must return only pro-tier doc, got {ids:?}"
    );
    let perf = body["performance"].as_object().expect("performance");
    let roundtrips = perf["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips");
    assert!(
        roundtrips >= 2 && roundtrips <= 4,
        "cold hybrid+filter should report batched roundtrips, got {roundtrips}"
    );
    let probed = perf["ann_probed_clusters"]
        .as_u64()
        .expect("ann_probed_clusters");
    assert!(probed >= 1, "hybrid cold query must probe ANN clusters, got {probed}");
}

/// Debug cold-plan endpoint matches [`plan_cold_query`] (no query execution).
#[tokio::test]
async fn cold_plan_debug_endpoint_on_minio() {
    use openpuffer::index::vector::cluster_get_upper_bound;
    use openpuffer::s3_batch::{build_cold_plan_debug, fetch_cold_vector_l0, ColdPlanOpts};

    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_COLD_PLAN_DEBUG;

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
                "id": "plan-a",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "cold plan debug alpha",
                    "tier": "pro"
                }
            }
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert_eq!(meta.index_cursor, meta.wal_commit_seq);

    let query_vec = vec![1.0_f64, 0.0, 0.0];
    let (l0_by_field, _, _) = fetch_cold_vector_l0(&fixture.client, &fixture.bucket, ns, &meta)
        .await
        .expect("L0 for planner");
    let expected = build_cold_plan_debug(
        ns,
        &meta,
        &[("embedding".into(), query_vec.clone())],
        &l0_by_field,
        ColdPlanOpts {
            include_wal_round: false,
            include_wal_tail: false,
        },
        "eventual",
        false,
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{}/v1/debug/namespaces/{}/cold-plan",
            serve.base_url,
            namespace_path_segment(ns)
        ))
        .json(&json!({
            "rank_by": ["vector", "ANN", "embedding", query_vec],
            "consistency": "eventual"
        }))
        .send()
        .await
        .expect("cold-plan debug POST");
    assert_eq!(resp.status(), 200, "cold-plan debug: {}", resp.text().await.unwrap_or_default());
    let body: Value = resp.json().await.expect("cold-plan json");

    assert_eq!(body["consistency"].as_str(), Some("eventual"));
    assert_eq!(
        body["storage_roundtrips"].as_u64(),
        Some(expected.storage_roundtrips as u64)
    );
    assert_eq!(
        body["round_key_counts"]["round2"].as_u64(),
        Some(expected.round_key_counts.round2 as u64)
    );
    assert_eq!(
        body["round_key_counts"]["round3"].as_u64(),
        Some(expected.round_key_counts.round3 as u64)
    );
    let probe = &body["probe_plan"][0];
    assert_eq!(probe["vector_field"].as_str(), Some("embedding"));
    assert_eq!(
        probe["round3_key_count"].as_u64(),
        Some(expected.probe_plan[0].round3_key_count as u64)
    );
    let l0 = l0_by_field
        .get("embedding")
        .expect("embedding L0")
        .clone()
        .clamp_probe_plan_for_query();
    assert_eq!(
        probe["cluster_get_upper_bound"].as_u64(),
        Some(cluster_get_upper_bound(&l0) as u64)
    );
    assert!(
        body["round_key_counts"]["round1"].as_u64() == Some(0),
        "eventual cold-plan must omit WAL round1"
    );
}

/// Cold path: hybrid `Product` (vector ∩ BM25) returns only docs matching both signals.
#[tokio::test]
async fn cold_hybrid_product_vector_on_minio() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_COLD_HYBRID_PRODUCT;

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
                "id": "cold-product-both",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "cold product alpha uniquepro",
                    "tier": "pro"
                }
            },
            {
                "id": "cold-product-bm25-only",
                "attributes": {
                    "embedding": [0.0, 0.0, 1.0],
                    "text": "cold product alpha uniquepro",
                    "tier": "pro"
                }
            },
            {
                "id": "cold-product-vector-only",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "cold product noise unrelated",
                    "tier": "pro"
                }
            }
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await;

    let body = query_response_ns(
        &serve.base_url,
        ns,
        json!({
            "rank_by": [
                "Product",
                ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
                ["BM25", "text", "uniquepro"]
            ],
            "top_k": 3,
            "consistency": "strong"
        }),
    )
    .await;
    let ids: Vec<String> = body["rows"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        ids,
        vec!["cold-product-both".to_string()],
        "Product cold query must return only doc in vector∩BM25 candidates, got {ids:?}"
    );
    let perf = body["performance"].as_object().expect("performance");
    let roundtrips = perf["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips");
    assert!(
        roundtrips >= 2 && roundtrips <= 4,
        "cold Product hybrid should report batched roundtrips, got {roundtrips}"
    );
    let probed = perf["ann_probed_clusters"]
        .as_u64()
        .expect("ann_probed_clusters");
    assert!(
        probed >= 1,
        "Product cold query must probe ANN clusters, got {probed}"
    );
}

/// Cold path: BM25-only `rank_by` + attribute filter — no vector probe round, roundtrips ≤ 4.
#[tokio::test]
async fn cold_fts_bm25_filter_on_minio() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_COLD_FTS_BM25;

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
                "id": "cold-fts-a",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "cold fts alpha stressterm",
                    "tier": "pro"
                }
            },
            {
                "id": "cold-fts-b",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "cold fts alpha stressterm",
                    "tier": "free"
                }
            },
            {
                "id": "cold-fts-c",
                "attributes": {
                    "embedding": [0.0, 0.0, 1.0],
                    "text": "cold fts bravo other",
                    "tier": "pro"
                }
            }
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await;

    let body = query_response_ns(
        &serve.base_url,
        ns,
        json!({
            "rank_by": ["BM25", "text", "alpha"],
            "filters": ["tier", "Eq", "pro"],
            "top_k": 5,
            "consistency": "strong"
        }),
    )
    .await;
    let ids: Vec<String> = body["rows"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        ids.contains(&"cold-fts-a".to_string()),
        "BM25+filter cold query must return pro-tier alpha doc, got {ids:?}"
    );
    assert!(
        !ids.contains(&"cold-fts-b".to_string()),
        "filter must exclude free-tier doc, got {ids:?}"
    );
    assert!(
        !ids.contains(&"cold-fts-c".to_string()),
        "BM25 alpha must not return bravo doc, got {ids:?}"
    );

    let perf = body["performance"].as_object().expect("performance");
    let roundtrips = perf["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips");
    assert!(
        roundtrips <= 4,
        "BM25-only cold storage_roundtrips {roundtrips} must be ≤ 4"
    );
    let probed = perf
        .get("ann_probed_clusters")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    assert_eq!(
        probed, 0,
        "BM25-only cold query must not probe ANN clusters, got {probed}"
    );
}

/// 10k indexed namespace: cold hybrid `Sum` (vector + BM25) + filter; FTS in bootstrap round 2.
#[tokio::test]
async fn cold_hybrid_10k_fts_vector_filter_on_minio() {
    use openpuffer::index::fts::FtsSegment;
    use openpuffer::s3_batch::{
        fetch_cold_index_bootstrap, fetch_cold_vector_l0, plan_cold_query, round2_bootstrap_keys,
        ColdPlanOpts,
    };

    let test_started = std::time::Instant::now();
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_COLD_HYBRID_10K;

    let serve = ServeHandle::spawn_with_cache(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
    );
    serve.wait_ready().await;

    let schema = json!({
        "text": {"type": "string", "full_text_search": true},
        "tier": {"type": "string", "filterable": true},
        "embedding": "[128]f32"
    });
    let batches = STRESS_DOCS / STRESS_BATCH;
    for b in 0..batches {
        if b > 0 {
            sleep(Duration::from_millis(1100)).await;
        }
        let start = b * STRESS_BATCH;
        let mut body = json!({ "upsert_columns": recall_filter_upsert_columns(start, STRESS_BATCH) });
        if b == 0 {
            body["schema"] = schema.clone();
        }
        write_batch(&serve.base_url, ns, body).await;
    }
    sleep(Duration::from_millis(1200)).await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(300)).await;

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert_eq!(
        meta.index_cursor, meta.wal_commit_seq,
        "namespace must be fully indexed before cold hybrid query"
    );
    assert!(
        meta.fts_segment_id > 0,
        "10k fixture must have FTS segment for hybrid BM25 leg"
    );

    let query_vec: Vec<f64> = (0..STRESS_DIM)
        .map(|d| (d as f64 * 0.02).cos())
        .collect();
    let fts_key = FtsSegment::key(ns, meta.fts_segment_id);
    let r2_keys = round2_bootstrap_keys(ns, &meta);
    assert!(
        r2_keys.iter().any(|k| k.contains("fts-")),
        "bootstrap round2 must list FTS key for hybrid cold, keys={r2_keys:?}"
    );
    assert!(
        r2_keys.contains(&fts_key),
        "bootstrap round2 must include latest fts segment {fts_key}"
    );

    let bootstrap = fetch_cold_index_bootstrap(&fixture.client, &fixture.bucket, ns, &meta)
        .await
        .expect("cold bootstrap fetch");
    assert!(
        bootstrap.fts.is_some(),
        "cold bootstrap on probed vector path must decode FTS index"
    );
    let (l0_by_field, _, _) = fetch_cold_vector_l0(&fixture.client, &fixture.bucket, ns, &meta)
        .await
        .expect("cold L0 fetch for hybrid planner");
    assert!(
        !l0_by_field.is_empty(),
        "hybrid cold planner needs L0 for probed round-3 keys"
    );
    let plan = plan_cold_query(
        ns,
        &meta,
        &[("embedding".into(), query_vec.clone())],
        &l0_by_field,
        None,
        ColdPlanOpts {
            include_wal_round: false,
            include_wal_tail: false,
        },
    );
    assert!(
        plan.round2_keys.iter().any(|k| k.contains("fts-")),
        "planner round2 must include FTS for hybrid cold, keys={:?}",
        plan.round2_keys
    );

    let client = reqwest::Client::new();
    client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");

    let body = query_response_ns(
        &serve.base_url,
        ns,
        json!({
            "rank_by": [
                "Sum",
                ["vector", "ANN", "embedding", query_vec],
                ["BM25", "text", "stressterm"]
            ],
            "filters": ["tier", "Eq", "pro"],
            "top_k": 10,
            "consistency": "strong"
        }),
    )
    .await;
    let rows = body["rows"].as_array().expect("hybrid rows");
    assert!(!rows.is_empty(), "cold hybrid+filter on 10k must return rows");
    for row in rows {
        let id = row["id"].as_str().expect("row id");
        let idx: usize = id
            .strip_prefix("doc-")
            .and_then(|s| s.parse().ok())
            .expect("doc id format doc-N");
        assert_eq!(
            idx % 20,
            0,
            "filter tier=pro must only return pro docs, got {id}"
        );
    }

    let perf = body["performance"].as_object().expect("performance");
    let roundtrips = perf["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips");
    assert!(
        roundtrips <= 4,
        "cold hybrid 10k storage_roundtrips {roundtrips} must be ≤ 4"
    );
    let probed = perf["ann_probed_clusters"]
        .as_u64()
        .expect("ann_probed_clusters");
    assert!(
        probed >= 1,
        "hybrid cold 10k must probe ANN clusters, got {probed}"
    );
    let ratio = perf["candidates_ratio"].as_f64().expect("candidates_ratio");
    assert!(
        ratio < 0.20,
        "cold hybrid candidates_ratio {ratio} should stay sub-linear on 10k"
    );

    assert!(
        test_started.elapsed() < Duration::from_secs(360),
        "cold_hybrid_10k test exceeded 360s wall clock"
    );
}

/// Cold path @ 10k: two vector columns — probed cluster GETs only for `rank_by` field B.
#[tokio::test]
async fn cold_two_vector_fields_query_probes_ranked_field_only() {
    use openpuffer::index::vector::CentroidIndexL0;
    use openpuffer::s3_batch::{
        build_cold_plan_debug, cluster_get_upper_bound, fetch_cold_vector_l0,
        fetch_cold_vector_probed, plan_cold_query, ColdPlanOpts,
    };

    let test_started = std::time::Instant::now();
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_COLD_TWO_VEC_10K;

    let serve = ServeHandle::spawn_with_cache(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
    );
    serve.wait_ready().await;

    let schema = json!({
        "embedding_a": "[128]f32",
        "embedding_b": "[128]f32"
    });
    let batches = STRESS_DOCS / STRESS_BATCH;
    for b in 0..batches {
        if b > 0 {
            sleep(Duration::from_millis(1100)).await;
        }
        let start = b * STRESS_BATCH;
        let mut body =
            json!({ "upsert_columns": stress_two_vector_upsert_columns(start, STRESS_BATCH) });
        if b == 0 {
            body["schema"] = schema.clone();
        }
        write_batch(&serve.base_url, ns, body).await;
    }
    sleep(Duration::from_millis(1200)).await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(300)).await;

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert_eq!(
        meta.index_cursor, meta.wal_commit_seq,
        "namespace must be fully indexed"
    );
    assert_meta_vector_fields(
        &meta,
        &[("embedding_a", STRESS_DIM as u32), ("embedding_b", STRESS_DIM as u32)],
    );

    let index_prefix = format!("{ROOT_PREFIX}{ns}/index/");
    let index_keys =
        list_keys_with_prefix(&fixture.client, &fixture.bucket, &index_prefix).await;
    let clusters_a = index_keys
        .iter()
        .filter(|k| k.contains("/index/embedding_a/") && k.contains("clusters-"))
        .count();
    let clusters_b = index_keys
        .iter()
        .filter(|k| k.contains("/index/embedding_b/") && k.contains("clusters-"))
        .count();
    assert!(
        clusters_a > 100 && clusters_b > 100,
        "both fields must have many cluster objects on S3 (a={clusters_a}, b={clusters_b})"
    );

    let l0_a_key = format!("{ROOT_PREFIX}{ns}/index/embedding_a/centroids-l0.bin");
    let l0_b_key = format!("{ROOT_PREFIX}{ns}/index/embedding_b/centroids-l0.bin");
    let l0_a = CentroidIndexL0::decode(&get_object_bytes(&fixture.client, &fixture.bucket, &l0_a_key).await)
        .expect("decode embedding_a L0")
        .clamp_probe_plan_for_query();
    let l0_b = CentroidIndexL0::decode(&get_object_bytes(&fixture.client, &fixture.bucket, &l0_b_key).await)
        .expect("decode embedding_b L0")
        .clamp_probe_plan_for_query();
    assert!(
        l0_a.num_fine_total > 100 && l0_b.num_fine_total > 100,
        "both fields need many fine clusters (a={}, b={})",
        l0_a.num_fine_total,
        l0_b.num_fine_total
    );

    let query_vec: Vec<f64> = (0..STRESS_DIM)
        .map(|d| (d as f64 * 0.02).cos())
        .collect();
    let max_cluster_gets_b = cluster_get_upper_bound(&l0_b);

    let (vindex_b, _, _, probed_b) = fetch_cold_vector_probed(
        &fixture.client,
        &fixture.bucket,
        ns,
        &meta,
        "embedding_b",
        l0_b.clone(),
        &query_vec,
    )
    .await
    .expect("probed fetch embedding_b");
    let fetched_b = vindex_b.clusters.len();
    assert!(
        fetched_b <= max_cluster_gets_b,
        "embedding_b probed cluster GETs {fetched_b} must be ≤ bound {max_cluster_gets_b}"
    );
    assert!(
        fetched_b < l0_b.num_fine_total as usize,
        "embedding_b probed {fetched_b} must be << num_fine_total {}",
        l0_b.num_fine_total
    );
    assert!(
        fetched_b * 10 < clusters_b,
        "embedding_b must not fetch all clusters: fetched {fetched_b}, s3 {clusters_b}"
    );
    assert!(
        fetched_b * 10 < clusters_a,
        "query on embedding_b must not pull embedding_a cluster segments (fetched_b={fetched_b}, s3_a={clusters_a})"
    );

    let (l0_by_field, _, _) = fetch_cold_vector_l0(&fixture.client, &fixture.bucket, ns, &meta)
        .await
        .expect("L0 for planner");
    let plan_b_only = plan_cold_query(
        ns,
        &meta,
        &[("embedding_b".into(), query_vec.clone())],
        &l0_by_field,
        None,
        ColdPlanOpts {
            include_wal_round: false,
            include_wal_tail: false,
        },
    );
    assert!(
        plan_b_only
            .round3_keys
            .iter()
            .all(|k| k.contains("/index/embedding_b/")),
        "round3 for embedding_b query must not include embedding_a keys: {:?}",
        plan_b_only.round3_keys
    );
    assert!(
        !plan_b_only
            .round3_keys
            .iter()
            .any(|k| k.contains("/index/embedding_a/")),
        "round3 must omit embedding_a clusters when ranking embedding_b"
    );

    let plan_both = plan_cold_query(
        ns,
        &meta,
        &[
            ("embedding_a".into(), query_vec.clone()),
            ("embedding_b".into(), query_vec.clone()),
        ],
        &l0_by_field,
        None,
        ColdPlanOpts {
            include_wal_round: false,
            include_wal_tail: false,
        },
    );
    assert!(
        plan_both
            .round3_keys
            .iter()
            .any(|k| k.contains("/index/embedding_a/")),
        "dual-probe plan must include embedding_a round3 keys"
    );
    assert!(
        plan_both
            .round3_keys
            .iter()
            .any(|k| k.contains("/index/embedding_b/")),
        "dual-probe plan must include embedding_b round3 keys"
    );

    let expected_debug = build_cold_plan_debug(
        ns,
        &meta,
        &[("embedding_b".into(), query_vec.clone())],
        &l0_by_field,
        ColdPlanOpts {
            include_wal_round: false,
            include_wal_tail: false,
        },
        "eventual",
        false,
    );
    assert_eq!(expected_debug.probe_plan.len(), 1);
    assert_eq!(
        expected_debug.probe_plan[0].vector_field, "embedding_b",
        "single-field rank_by must plan one vector probe"
    );
    assert!(
        expected_debug.probe_plan[0].round3_key_count > 0,
        "embedding_b probe must have round3 keys"
    );
    assert_eq!(
        expected_debug.round_key_counts.round3,
        plan_b_only.round3_keys.len()
    );

    let expected_dual = build_cold_plan_debug(
        ns,
        &meta,
        &[
            ("embedding_a".into(), query_vec.clone()),
            ("embedding_b".into(), query_vec.clone()),
        ],
        &l0_by_field,
        ColdPlanOpts {
            include_wal_round: false,
            include_wal_tail: false,
        },
        "eventual",
        false,
    );
    assert_eq!(
        expected_dual.probe_plan.len(),
        2,
        "dual vector probes must expose two probe_plan entries"
    );
    for field in ["embedding_a", "embedding_b"] {
        let probe = expected_dual
            .probe_plan
            .iter()
            .find(|p| p.vector_field == field)
            .unwrap_or_else(|| panic!("probe_plan missing {field}"));
        assert!(
            probe.round3_key_count > 0,
            "{field} must have round3 keys in dual-probe plan"
        );
    }

    let client = reqwest::Client::new();
    client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");

    let debug_resp = client
        .post(format!(
            "{}/v1/debug/namespaces/{}/cold-plan",
            serve.base_url,
            namespace_path_segment(ns)
        ))
        .json(&json!({
            "rank_by": ["vector", "ANN", "embedding_b", query_vec],
            "consistency": "eventual"
        }))
        .send()
        .await
        .expect("cold-plan debug POST");
    assert_eq!(
        debug_resp.status(),
        200,
        "cold-plan debug: {}",
        debug_resp.text().await.unwrap_or_default()
    );
    let debug_body: Value = debug_resp.json().await.expect("cold-plan json");
    assert_eq!(
        debug_body["round_key_counts"]["round3"].as_u64(),
        Some(expected_debug.round_key_counts.round3 as u64)
    );
    let probe = &debug_body["probe_plan"][0];
    assert_eq!(probe["vector_field"].as_str(), Some("embedding_b"));
    assert_eq!(
        probe["round3_key_count"].as_u64(),
        Some(expected_debug.probe_plan[0].round3_key_count as u64)
    );
    assert_eq!(
        probe["cluster_get_upper_bound"].as_u64(),
        Some(max_cluster_gets_b as u64)
    );

    let body = query_response_ns(
        &serve.base_url,
        ns,
        json!({
            "rank_by": ["vector", "ANN", "embedding_b", query_vec],
            "top_k": 10,
            "consistency": "eventual"
        }),
    )
    .await;
    let perf = body["performance"].as_object().expect("performance");
    let probed = perf["ann_probed_clusters"]
        .as_u64()
        .expect("ann_probed_clusters");
    assert_eq!(
        probed, probed_b as u64,
        "HTTP cold query probed clusters must match direct probed fetch"
    );
    assert!(
        probed >= 1 && (probed as usize) <= max_cluster_gets_b,
        "ann_probed_clusters {probed} must be within probe bound {max_cluster_gets_b}"
    );
    let ratio = perf["candidates_ratio"].as_f64().expect("candidates_ratio");
    assert!(
        ratio < 0.20,
        "cold ANN on embedding_b candidates_ratio {ratio} should stay sub-linear on 10k"
    );
    assert!(
        body["rows"].as_array().map(|a| !a.is_empty()).unwrap_or(false),
        "cold ANN on embedding_b must return rows"
    );

    assert!(
        test_started.elapsed() < Duration::from_secs(360),
        "cold_two_vector_fields 10k test exceeded 360s wall clock"
    );
}

/// Cold query on a namespace that was indexed then emptied returns zero rows (not an error).
#[tokio::test]
async fn cold_query_empty_indexed_namespace_returns_empty_rows() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_COLD_EMPTY_DOCS;

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
                "id": "gone-a",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "ephemeral alpha"
                }
            },
            {
                "id": "gone-b",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "ephemeral bravo"
                }
            }
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    let del = reqwest::Client::new()
        .post(format!("{}/v2/namespaces/{ns}", serve.base_url))
        .json(&json!({ "deletes": ["gone-a", "gone-b"] }))
        .send()
        .await
        .expect("delete batch");
    assert_eq!(del.status(), StatusCode::OK);
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await;

    let body = query_response_ns(
        &serve.base_url,
        ns,
        json!({
            "rank_by": ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            "top_k": 5,
            "consistency": "strong"
        }),
    )
    .await;
    assert!(
        body["rows"].as_array().map(|a| a.is_empty()).unwrap_or(true),
        "empty namespace cold query must return no rows, got {:?}",
        body["rows"]
    );
    let perf = body["performance"].as_object().expect("performance");
    assert_eq!(
        perf["approx_namespace_size"].as_u64(),
        Some(0),
        "performance must report empty namespace size"
    );
}

/// Strong cold query while index lags: hybrid + filter sees unindexed tail doc matching tier.
#[tokio::test]
async fn cold_strong_index_lag_hybrid_filter_tail_on_minio() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_COLD_INDEX_LAG_FILTER;

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
                "id": "indexed-pro",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "indexed baseline alpha",
                    "tier": "pro"
                }
            },
            {
                "id": "indexed-free",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "indexed baseline bravo",
                    "tier": "free"
                }
            }
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    upsert_batch(
        &serve.base_url,
        ns,
        json!([{
            "id": "tail-pro-hybrid",
            "attributes": {
                "embedding": [0.99, 0.01, 0.0],
                "text": "tail alpha stressterm unindexed",
                "tier": "pro"
            }
        }]),
    )
    .await;

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, ns).await;
    assert!(
        meta.index_cursor < meta.wal_commit_seq,
        "index lag required: cursor={} commit={}",
        meta.index_cursor,
        meta.wal_commit_seq
    );

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await;

    let body = query_response_ns(
        &serve.base_url,
        ns,
        json!({
            "rank_by": [
                "Sum",
                ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
                ["BM25", "text", "alpha"]
            ],
            "filters": ["tier", "Eq", "pro"],
            "top_k": 5,
            "consistency": "strong"
        }),
    )
    .await;
    let ids: Vec<String> = body["rows"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        ids.contains(&"tail-pro-hybrid".to_string()),
        "strong cold hybrid+filter must include unindexed pro tail doc, got {ids:?}"
    );
    assert!(
        !ids.contains(&"indexed-free".to_string()),
        "filter tier=pro must exclude free-tier indexed doc, got {ids:?}"
    );
    let perf = body["performance"].as_object().expect("performance");
    let exhaustive = perf["exhaustive_search_count"]
        .as_u64()
        .expect("exhaustive_search_count");
    assert!(
        exhaustive >= 1,
        "tail doc should be scored exhaustively during index lag, got {exhaustive}"
    );
    let roundtrips = perf["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips");
    assert!(
        roundtrips >= 3 && roundtrips <= 5,
        "index-lag cold hybrid should use bootstrap + probe + WAL tail, got {roundtrips}"
    );
}

/// Cold query on a namespace that does not exist returns 404.
#[tokio::test]
async fn cold_query_nonexistent_namespace_returns_not_found() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());

    let serve = ServeHandle::spawn_with_cache(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
    );
    serve.wait_ready().await;

    let resp = reqwest::Client::new()
        .post(format!(
            "{}/v2/namespaces/{}/query",
            serve.base_url,
            namespace_path_segment(NAMESPACE_NONEXISTENT_COLD)
        ))
        .json(&json!({
            "rank_by": ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            "top_k": 1
        }))
        .send()
        .await
        .expect("cold query missing namespace");
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "query on missing namespace must be 404, body={}",
        resp.text().await.unwrap_or_default()
    );
}

/// ANN v3 index build on MinIO: cold vector query + dual-read of v2-shaped L0 bytes.
#[tokio::test]
async fn s3_ann_v3_cold_query_and_v2_l0_dual_read_on_minio() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_S3_V3_ANN;

    let serve = ServeHandle::spawn_with_limits_and_ann_version(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
        None,
        None,
        None,
        None,
        None,
        Some(3),
    );
    serve.wait_ready().await;

    upsert_batch(
        &serve.base_url,
        ns,
        json!([
            {
                "id": "v3-a",
                "attributes": {
                    "embedding": [1.0, 0.0, 0.0],
                    "text": "ann v3 alpha"
                }
            },
            {
                "id": "v3-b",
                "attributes": {
                    "embedding": [0.0, 1.0, 0.0],
                    "text": "ann v3 bravo"
                }
            }
        ]),
    )
    .await;
    wait_until_indexed(&serve.base_url, ns, Duration::from_secs(30)).await;

    let l0_key = centroids_l0_s3_key(ns, "embedding");
    let l0_bytes = get_object_bytes(&fixture.client, &fixture.bucket, &l0_key).await;
    let l0 = openpuffer::index::vector::CentroidIndexL0::decode(&l0_bytes)
        .expect("decode centroids-l0");
    assert_eq!(
        l0.ann_version, 3,
        "v3 server must write ann_version=3 in L0 metadata"
    );

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await;

    let body = query_response_ns(
        &serve.base_url,
        ns,
        json!({
            "rank_by": ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            "top_k": 2,
            "consistency": "strong"
        }),
    )
    .await;
    let ids: Vec<String> = body["rows"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        ids.first().map(String::as_str),
        Some("v3-a"),
        "v3 cold ANN query should rank v3-a first, got {ids:?}"
    );
    let perf = body["performance"].as_object().expect("performance");
    assert!(
        perf["ann_probed_clusters"].as_u64().unwrap_or(0) >= 1,
        "v3 cold query must report probed clusters"
    );
}

/// WAL compaction on MinIO: old segment deleted, snapshot.bin present, decode + query still correct.
#[tokio::test]
async fn s3_compaction_removes_old_wal_objects() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let ns = NAMESPACE_S3_COMPACT;

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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
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
    let wal_keys = list_keys_with_prefix_until(
        &fixture.client,
        &fixture.bucket,
        &wal_prefix,
        Duration::from_secs(30),
        |keys| keys.iter().any(|k| k == &snapshot_key),
    )
    .await;
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

    serve.stop();
    // Cold batched load (`--cache-dir=""`) on a fresh process; must not hit warm server on same port.
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

    let client = reqwest::Client::new();
    let reset = client
        .post(format!("{}/v1/debug/cache-stats/reset", serve_cold.base_url))
        .send()
        .await
        .expect("cache reset");
    assert_eq!(reset.status(), StatusCode::OK);

    let cold_body = query_response_ns(
        &serve_cold.base_url,
        ns,
        json!({
            "rank_by": ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
            "top_k": 10,
            "consistency": "strong"
        }),
    )
    .await;
    let vector_ids: Vec<String> = cold_body["rows"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        vector_ids.first().map(String::as_str),
        Some("s3c-0"),
        "cold ANN top-1 after s3 compaction, ids={vector_ids:?}"
    );
    let perf = cold_body["performance"].as_object().expect("performance");
    let roundtrips = perf
        .get("storage_roundtrips")
        .and_then(|v| v.as_u64())
        .expect("storage_roundtrips on cold ANN query after compaction");
    assert!(
        roundtrips <= 4,
        "storage_roundtrips {roundtrips} must be ≤ 4 after compaction cold restart"
    );
    let probed = perf
        .get("ann_probed_clusters")
        .and_then(|v| v.as_u64())
        .expect("ann_probed_clusters on cold ANN query after compaction");
    assert!(
        probed >= 1,
        "probed cluster metrics required on cold ANN after compaction, got {probed}"
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

    let deadline = Duration::from_secs(120);
    let wait_all = async {
        wait_until_indexed(&base, NAMESPACE_FAIR_HOT, deadline).await;
        wait_until_indexed(&base, NAMESPACE_FAIR_B, deadline).await;
        wait_until_indexed(&base, NAMESPACE_FAIR_C, deadline).await;
    };
    tokio::time::timeout(deadline, wait_all)
        .await
        .expect("all three namespaces should index within 120s");

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
    assert!(
        !src_keys.is_empty(),
        "source namespace must have S3 objects before copy"
    );
    let dest_keys = list_keys_with_prefix_min_count(
        &fixture.client,
        &fixture.bucket,
        &dest_prefix,
        src_keys.len(),
        Duration::from_secs(45),
    )
    .await;

    assert_eq!(
        src_keys.len(),
        dest_keys.len(),
        "copy must duplicate every source key (src={src_keys:?} dest={dest_keys:?})"
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

/// Valid WAL on S3, then corrupt `wal/{seq:08}.bin` via PutObject; fail policy errors query, skip omits segment.
#[tokio::test]
async fn corrupt_wal_segment_on_minio_fail_and_skip_policies() {
    let fixture = S3Fixture::from_testcontainers().await;

    async fn seed_two_wal_segments(base_url: &str, namespace: &str) {
        write_batch(
            base_url,
            namespace,
            json!({
                "schema": { "text": { "type": "string", "full_text_search": true } },
                "upsert_rows": [{
                    "id": "wal-good",
                    "attributes": { "text": "corruptwal goodterm walseq1" }
                }]
            }),
        )
        .await;
        sleep(Duration::from_millis(1500)).await;
        write_batch(
            base_url,
            namespace,
            json!({
                "upsert_rows": [{
                    "id": "wal-bad",
                    "attributes": { "text": "corruptwal badterm walseq2" }
                }]
            }),
        )
        .await;
        sleep(Duration::from_millis(500)).await;
    }

    // Default OPENPUFFER_WAL_CORRUPT_POLICY=fail aborts namespace load on corrupt tail segment.
    {
        let listen = format!("127.0.0.1:{}", free_port());
        let mut serve = ServeHandle::spawn_with_options(
            &fixture,
            &listen,
            Some(PathBuf::from("")),
            Some(1),
            None,
        );
        serve.wait_ready().await;
        seed_two_wal_segments(&serve.base_url, NAMESPACE_WAL_CORRUPT_FAIL).await;

        let meta =
            fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_WAL_CORRUPT_FAIL).await;
        assert!(
            meta.wal_commit_seq >= 2,
            "expected two WAL commits, meta={meta:?}"
        );
        decode_wal_entry_from_s3(
            &fixture.client,
            &fixture.bucket,
            NAMESPACE_WAL_CORRUPT_FAIL,
            1,
        )
        .await;
        decode_wal_entry_from_s3(
            &fixture.client,
            &fixture.bucket,
            NAMESPACE_WAL_CORRUPT_FAIL,
            2,
        )
        .await;

        corrupt_wal_crc_byte_on_s3(
            &fixture.client,
            &fixture.bucket,
            NAMESPACE_WAL_CORRUPT_FAIL,
            2,
        )
        .await;

        serve.stop();
        drop(serve);
        sleep(Duration::from_millis(300)).await;

        let serve_fail = ServeHandle::spawn_with_limits(
            &fixture,
            &listen,
            Some(PathBuf::from("")),
            None,
            None,
            None,
            None,
            None,
        );
        serve_fail.wait_ready().await;

        let (status, body) = query_expect(
            &serve_fail.base_url,
            NAMESPACE_WAL_CORRUPT_FAIL,
            r#"{"rank_by": ["BM25", "text", "goodterm"], "top_k": 5}"#,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "fail policy should abort WAL replay: {body:?}"
        );
        assert_api_error_shape(&body);
        assert!(
            body["error"]
                .as_str()
                .unwrap_or("")
                .to_ascii_lowercase()
                .contains("corrupt"),
            "expected corrupt WAL error, got {body:?}"
        );
    }

    // skip policy logs and continues without applying the corrupt segment.
    {
        let listen = format!("127.0.0.1:{}", free_port());
        let mut serve = ServeHandle::spawn_with_options(
            &fixture,
            &listen,
            Some(PathBuf::from("")),
            Some(1),
            None,
        );
        serve.wait_ready().await;
        seed_two_wal_segments(&serve.base_url, NAMESPACE_WAL_CORRUPT_SKIP).await;

        corrupt_wal_crc_byte_on_s3(
            &fixture.client,
            &fixture.bucket,
            NAMESPACE_WAL_CORRUPT_SKIP,
            2,
        )
        .await;

        serve.stop();
        drop(serve);
        sleep(Duration::from_millis(300)).await;

        let serve_skip = ServeHandle::spawn_with_limits(
            &fixture,
            &listen,
            Some(PathBuf::from("")),
            None,
            None,
            None,
            None,
            Some("skip"),
        );
        serve_skip.wait_ready().await;

        let good_ids = query_ids_ns(
            &serve_skip.base_url,
            NAMESPACE_WAL_CORRUPT_SKIP,
            json!(["BM25", "text", "goodterm"]),
            None,
        )
        .await;
        assert!(
            good_ids.contains(&"wal-good".to_string()),
            "seq 1 doc should be visible under skip policy, ids={good_ids:?}"
        );

        let bad_ids = query_ids_ns(
            &serve_skip.base_url,
            NAMESPACE_WAL_CORRUPT_SKIP,
            json!(["BM25", "text", "badterm"]),
            None,
        )
        .await;
        assert!(
            !bad_ids.contains(&"wal-bad".to_string()),
            "corrupt seq 2 must be skipped, ids={bad_ids:?}"
        );
    }
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
        None,
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

/// `delete_by_filter_allow_partial` / `patch_by_filter_allow_partial` cap at max filter batch and set `rows_remaining`.
#[tokio::test]
async fn filter_batch_partial_delete_and_patch_on_minio() {
    const NS: &str = "itest-filter-partial";
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn_with_limits(
        &fixture,
        &listen,
        None,
        None,
        Some(50),
        None,
        Some(2),
        None,
    );
    serve.wait_ready().await;

    write_batch(
        &serve.base_url,
        NS,
        json!({
            "schema": { "tag": { "type": "string", "filterable": true } },
            "block_until_indexed": true,
            "upsert_rows": [
                { "id": "p1", "attributes": { "tag": "bulk" } },
                { "id": "p2", "attributes": { "tag": "bulk" } },
                { "id": "p3", "attributes": { "tag": "bulk" } },
                { "id": "p4", "attributes": { "tag": "bulk" } }
            ]
        }),
    )
    .await;

    let (status, body) = write_expect(
        &serve.base_url,
        NS,
        json!({
            "delete_by_filter": ["tag", "Eq", "bulk"],
            "delete_by_filter_allow_partial": true
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "partial delete batch 1: {body:?}");
    assert_eq!(body["rows_deleted"].as_u64(), Some(2));
    assert_eq!(body["rows_remaining"].as_bool(), Some(true));

    let wal1 = decode_wal_entry_from_s3(&fixture.client, &fixture.bucket, NS, 2).await;
    assert_eq!(
        wal1.deletes.len(),
        2,
        "S3 WAL seq 2 must record exactly two deletes, got {:?}",
        wal1.deletes
    );

    sleep(Duration::from_millis(1200)).await;

    let (status, body) = write_expect(
        &serve.base_url,
        NS,
        json!({
            "delete_by_filter": ["tag", "Eq", "bulk"],
            "delete_by_filter_allow_partial": true
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "partial delete batch 2: {body:?}");
    assert_eq!(body["rows_deleted"].as_u64(), Some(2));
    assert!(
        body.get("rows_remaining").is_none() || body["rows_remaining"].as_bool() == Some(false),
        "final partial batch should not set rows_remaining: {body:?}"
    );

    wait_until_indexed(&serve.base_url, NS, Duration::from_secs(60)).await;

    let remaining = export_all_ids(&serve.base_url, NS, None).await;
    assert!(
        remaining.is_empty(),
        "two partial delete batches (cap 2) must remove all four docs, got {remaining:?}"
    );

    write_batch(
        &serve.base_url,
        NS,
        json!({
            "block_until_indexed": true,
            "upsert_rows": [
                { "id": "q1", "attributes": { "tag": "patch", "tier": "a" } },
                { "id": "q2", "attributes": { "tag": "patch", "tier": "a" } },
                { "id": "q3", "attributes": { "tag": "patch", "tier": "a" } },
                { "id": "q4", "attributes": { "tag": "patch", "tier": "a" } }
            ]
        }),
    )
    .await;

    let client = reqwest::Client::new();
    let patch_resp = client
        .post(format!(
            "{}/v2/namespaces/{}",
            serve.base_url,
            namespace_path_segment(NS)
        ))
        .json(&json!({
            "patch_by_filter": {
                "filters": ["tag", "Eq", "patch"],
                "patch": { "tier": "b" }
            },
            "patch_by_filter_allow_partial": true,
            "block_until_indexed": true
        }))
        .send()
        .await
        .expect("patch_by_filter partial");
    assert_eq!(
        patch_resp.status(),
        StatusCode::OK,
        "partial patch failed"
    );
    let patch_body: Value = patch_resp.json().await.expect("patch json");
    assert_eq!(patch_body["rows_patched"].as_u64(), Some(2));
    assert_eq!(patch_body["rows_remaining"].as_bool(), Some(true));

    let wal_keys = list_wal_keys(&fixture.client, &fixture.bucket, NS).await;
    let seqs = wal_segment_seqs(&wal_keys);
    let mut patch_wal = None;
    for &seq in seqs.iter().rev() {
        let entry = decode_wal_entry_from_s3(&fixture.client, &fixture.bucket, NS, seq).await;
        if entry.patches.len() == 2 {
            patch_wal = Some(entry);
            break;
        }
    }
    let wal_patch = patch_wal.expect("S3 WAL segment with two partial patches");
    assert_eq!(
        wal_patch.patches.len(),
        2,
        "partial patch batch must write two patches to S3, ids: {:?}",
        wal_patch
            .patches
            .iter()
            .map(|p| p.id.as_str())
            .collect::<Vec<_>>()
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
        json!({
            "delete_by_filter": ["tier", "Eq", "upgraded"],
            "block_until_indexed": true
        }),
    )
    .await;

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
    let compact_deadline = tokio::time::Instant::now() + Duration::from_secs(120);
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
    let src_key_count = list_keys_with_prefix(&fixture.client, &fixture.bucket, &format!("{ROOT_PREFIX}{ns}/")).await.len();
    let branch_keys = list_keys_with_prefix_min_count(
        &fixture.client,
        &fixture.bucket,
        &branch_prefix,
        src_key_count,
        Duration::from_secs(45),
    )
    .await;
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