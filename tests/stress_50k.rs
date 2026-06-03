//! Optional 50k-document namespace stress test (MinIO via testcontainers).
//!
//! Not run in default CI (`#[ignore]`). Enable with:
//! `cargo test -F large_stress --test stress_50k -- --ignored --nocapture`
//!
//! Writes respect the ~1 WAL commit/s/namespace rate (1.1s between column batches).
//! Run with `--release` so indexing finishes within the 300s wall timeout.

mod common;

use common::s3_harness::*;
use openpuffer::models::ROOT_PREFIX;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;

const NAMESPACE: &str = "itest-50k-stress";
const STRESS_DOCS: usize = 50_000;
/// Max upsert rows per WAL batch (server default 10k); 5 commits vs 25×2k keeps wall time under 300s.
const STRESS_BATCH: usize = 10_000;
const STRESS_DIM: usize = 128;
/// Looser than 10k integration (<0.15); ANN probe fraction grows slowly with N.
const MAX_CANDIDATES_RATIO: f64 = 0.20;
const TEST_WALL_TIMEOUT: Duration = Duration::from_secs(300);
const INDEX_CATCHUP_TIMEOUT: Duration = Duration::from_secs(280);

fn stress_upsert_columns(start: usize, count: usize) -> Value {
    let mut ids = Vec::with_capacity(count);
    let mut texts = Vec::with_capacity(count);
    let mut embeddings = Vec::with_capacity(count);
    for i in start..start + count {
        ids.push(json!(format!("doc-{i}")));
        texts.push(json!(format!("stressterm50k document number {i}")));
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

/// 50k-column upsert (25×2k batches), background index, warm, ANN under candidate-ratio guard.
#[tokio::test]
#[ignore = "optional large stress; run: cargo test -F large_stress --test stress_50k -- --ignored"]
async fn fifty_thousand_docs_indexed_query() {
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
        write_batch(&serve.base_url, NAMESPACE, body).await;
    }
    sleep(Duration::from_millis(1200)).await;
    let write_elapsed = write_started.elapsed();

    wait_until_indexed(&serve.base_url, NAMESPACE, INDEX_CATCHUP_TIMEOUT).await;
    let index_elapsed = write_started.elapsed();

    let warm_resp = reqwest::Client::new()
        .post(format!(
            "{}/v1/namespaces/{NAMESPACE}/warm",
            serve.base_url
        ))
        .send()
        .await
        .expect("warm request");
    assert_eq!(warm_resp.status(), reqwest::StatusCode::OK);

    let query_vec: Vec<f64> = (0..STRESS_DIM)
        .map(|d| (d as f64 * 0.02).cos())
        .collect();
    let vector_resp = query_response_ns(
        &serve.base_url,
        NAMESPACE,
        json!({
            "rank_by": ["vector", "ANN", "embedding", query_vec],
            "top_k": 10
        }),
    )
    .await;
    let rows = vector_resp["rows"].as_array().expect("vector rows");
    assert!(!rows.is_empty(), "vector query returned no rows");
    assert!(rows.len() <= 10, "top_k=10 but got {} rows", rows.len());
    let perf = vector_resp["performance"].as_object().expect("performance");
    let ratio = perf["candidates_ratio"].as_f64().expect("candidates_ratio");
    assert!(
        ratio < MAX_CANDIDATES_RATIO,
        "candidates_ratio {ratio} must be < {MAX_CANDIDATES_RATIO} for 50k indexed ANN"
    );
    assert_eq!(
        perf["approx_namespace_size"].as_u64(),
        Some(STRESS_DOCS as u64)
    );

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE).await;
    assert_eq!(
        meta.index_cursor, meta.wal_commit_seq,
        "index must catch up: index_cursor={} wal_commit_seq={}",
        meta.index_cursor, meta.wal_commit_seq
    );
    assert!(meta.wal_commit_seq >= 1);

    let l0_key = format!("{ROOT_PREFIX}{NAMESPACE}/index/embedding/centroids-l0.bin");
    let l0_size = object_size(&fixture.client, &fixture.bucket, &l0_key).await;
    assert!(
        l0_size > 0,
        "centroids-l0.bin must exist and be non-empty on MinIO after 50k index, size={l0_size}"
    );

    let total_elapsed = test_started.elapsed();
    eprintln!(
        "fifty_thousand_docs_indexed_query: writes={write_elapsed:?} index+query={index_elapsed:?} total={total_elapsed:?} wal_commits={} candidates_ratio={ratio:.4} l0_bytes={l0_size}",
        meta.wal_commit_seq
    );
    assert!(
        total_elapsed < TEST_WALL_TIMEOUT,
        "test exceeded {TEST_WALL_TIMEOUT:?} wall clock (actual {total_elapsed:?})"
    );
}