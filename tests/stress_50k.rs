//! Optional 50k-document namespace stress tests (MinIO via testcontainers).
//!
//! Not run in default CI (`#[ignore]`). Enable with:
//! `cargo test --release -F large_stress --test stress_50k -- --ignored --nocapture`
//!
//! Writes respect the ~1 WAL commit/s/namespace rate (1.1s between column batches).
//! Run with `--release` so indexing finishes within the 300s wall timeout.
//!
//! | Test | Purpose |
//! |------|---------|
//! | `fifty_thousand_docs_indexed_query` | v2 default, warm ANN `candidates_ratio` |
//! | `fifty_thousand_docs_v3_cold_probed_validation` | v3 index, strong cold probed path (roundtrips, recall, ratio) |
//! | `v3_cold_probed_wiring_at_2k` | Fast wiring proof when 50k ingest is unavailable |

mod common;

use common::s3_harness::*;
use openpuffer::index::vector::brute_force_top_k;
use openpuffer::meta::DistanceMetric;
use openpuffer::models::ROOT_PREFIX;
use reqwest::StatusCode;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::time::sleep;

const NAMESPACE_WARM: &str = "itest-50k-stress";
const NAMESPACE_V3_COLD: &str = "itest-50k-v3-cold";
const NAMESPACE_WIRING: &str = "itest-2k-v3-cold-wiring";
const STRESS_DOCS: usize = 50_000;
const WIRING_DOCS: usize = 2_000;
/// Max upsert rows per WAL batch (server default 10k); 5 commits vs 25×2k keeps wall time under 300s.
const STRESS_BATCH: usize = 10_000;
const STRESS_DIM: usize = 128;
/// Looser than 10k integration (<0.15); ANN probe fraction grows slowly with N.
const MAX_CANDIDATES_RATIO: f64 = 0.20;
/// Mid-tier recall gate between 10k integration (0.85) and 100k nightly (0.88).
const MIN_RECALL_AT_10_50K: f64 = 0.86;
const MAX_STORAGE_ROUNDTRIPS: u64 = 4;
const COLD_QUERY_RUNS: usize = 7;
const RECALL_QUERIES: usize = 10;
const RECALL_TOP_K: usize = 10;
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

fn synthetic_embedding(doc_index: usize) -> Vec<f64> {
    (0..STRESS_DIM)
        .map(|d| ((doc_index * STRESS_DIM + d) as f64 * 0.001).sin())
        .collect()
}

fn percentile_ms(samples: &mut [u64], pct: u32) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    samples.sort_unstable();
    let n = samples.len();
    let mut idx = (n * pct as usize + 99) / 100;
    if idx == 0 {
        idx = 1;
    }
    idx -= 1;
    if idx >= n {
        idx = n - 1;
    }
    samples[idx]
}

fn latency_percentiles_ms(samples: &[u64]) -> (u64, u64, u64) {
    let mut sorted = samples.to_vec();
    (
        percentile_ms(&mut sorted.clone(), 50),
        percentile_ms(&mut sorted.clone(), 90),
        percentile_ms(&mut sorted, 99),
    )
}

fn count_ann_index_objects(keys: &[String]) -> usize {
    keys.iter()
        .filter(|k| {
            k.contains("clusters-") || (k.contains("centroids-l1-") && k.ends_with(".bin"))
        })
        .count()
}

async fn ingest_namespace(
    serve: &ServeHandle,
    namespace: &str,
    docs: usize,
    batch_size: usize,
) -> u64 {
    let schema = json!({
        "text": {"type": "string", "full_text_search": true},
        "embedding": "[128]f32"
    });
    let batches = docs / batch_size;
    assert_eq!(batches * batch_size, docs, "docs must divide batch_size");
    for b in 0..batches {
        if b > 0 {
            sleep(Duration::from_millis(1100)).await;
        }
        let start = b * batch_size;
        let mut body = json!({ "upsert_columns": stress_upsert_columns(start, batch_size) });
        if b == 0 {
            body["schema"] = schema.clone();
        }
        write_batch(&serve.base_url, namespace, body).await;
    }
    sleep(Duration::from_millis(1200)).await;
    wait_until_indexed(&serve.base_url, namespace, INDEX_CATCHUP_TIMEOUT).await;
    docs as u64
}

async fn recall_at_10_on_namespace(serve: &ServeHandle, namespace: &str, docs: usize) -> f64 {
    let mut vectors: Vec<(String, Vec<f64>)> = Vec::with_capacity(docs);
    for i in 0..docs {
        vectors.push((format!("doc-{i}"), synthetic_embedding(i)));
    }
    let metric = DistanceMetric::CosineDistance;
    let client = reqwest::Client::new();
    let mut recall_sum = 0.0f64;
    let stride = docs / RECALL_QUERIES;
    for q in 0..RECALL_QUERIES {
        let query = vectors[q * stride].1.clone();
        let brute = brute_force_top_k(&vectors, &query, metric, RECALL_TOP_K);
        client
            .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
            .send()
            .await
            .expect("cache reset");
        let resp = client
            .post(format!(
                "{}/v2/namespaces/{}/query",
                serve.base_url,
                namespace_path_segment(namespace)
            ))
            .json(&json!({
                "rank_by": ["vector", "ANN", "embedding", query],
                "top_k": RECALL_TOP_K,
                "consistency": "strong"
            }))
            .send()
            .await
            .expect("recall query");
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = resp.json().await.expect("query json");
        let rows = body["rows"].as_array().expect("rows");
        let ann_set: HashSet<String> = rows
            .iter()
            .filter_map(|r| r["id"].as_str().map(str::to_string))
            .collect();
        let hits = brute.iter().filter(|id| ann_set.contains(*id)).count();
        recall_sum += hits as f64 / RECALL_TOP_K as f64;
    }
    recall_sum / RECALL_QUERIES as f64
}

async fn cold_vector_query_ms(serve: &ServeHandle, namespace: &str) -> (u64, Value) {
    let client = reqwest::Client::new();
    client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");
    let query_vec: Vec<f64> = (0..STRESS_DIM)
        .map(|d| (d as f64 * 0.02).cos())
        .collect();
    let t0 = Instant::now();
    let resp = client
        .post(format!(
            "{}/v2/namespaces/{}/query",
            serve.base_url,
            namespace_path_segment(namespace)
        ))
        .json(&json!({
            "rank_by": ["vector", "ANN", "embedding", query_vec],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("cold query");
    assert_eq!(resp.status(), StatusCode::OK, "cold query failed");
    let ms = t0.elapsed().as_millis() as u64;
    let body = resp.json().await.expect("query json");
    (ms, body)
}

async fn assert_v3_l0_on_s3(fixture: &S3Fixture, namespace: &str) {
    let l0_key = format!("{ROOT_PREFIX}{namespace}/index/embedding/centroids-l0.bin");
    let l0_bytes = get_object_bytes(&fixture.client, &fixture.bucket, &l0_key).await;
    let l0 = openpuffer::index::vector::CentroidIndexL0::decode(&l0_bytes)
        .expect("decode centroids-l0");
    assert_eq!(
        l0.ann_version, 3,
        "OPENPUFFER_ANN_VERSION=3 serve must write ann_version=3 in L0"
    );
}

/// 50k-column upsert (5×10k batches), background index, warm, ANN under candidate-ratio guard (v2 default).
#[tokio::test]
#[ignore = "optional large stress; run: cargo test --release -F large_stress --test stress_50k fifty_thousand_docs_indexed_query -- --ignored --nocapture"]
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

    let write_started = std::time::Instant::now();
    ingest_namespace(&serve, NAMESPACE_WARM, STRESS_DOCS, STRESS_BATCH).await;
    let write_elapsed = write_started.elapsed();

    let warm_resp = reqwest::Client::new()
        .post(format!(
            "{}/v1/namespaces/{NAMESPACE_WARM}/warm",
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
        NAMESPACE_WARM,
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

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_WARM).await;
    assert_eq!(
        meta.index_cursor, meta.wal_commit_seq,
        "index must catch up: index_cursor={} wal_commit_seq={}",
        meta.index_cursor, meta.wal_commit_seq
    );
    assert!(meta.wal_commit_seq >= 1);

    let l0_key = format!("{ROOT_PREFIX}{NAMESPACE_WARM}/index/embedding/centroids-l0.bin");
    let l0_size = object_size(&fixture.client, &fixture.bucket, &l0_key).await;
    assert!(
        l0_size > 0,
        "centroids-l0.bin must exist and be non-empty on MinIO after 50k index, size={l0_size}"
    );

    let total_elapsed = test_started.elapsed();
    eprintln!(
        "fifty_thousand_docs_indexed_query: writes+index={write_elapsed:?} total={total_elapsed:?} wal_commits={} candidates_ratio={ratio:.4} l0_bytes={l0_size}",
        meta.wal_commit_seq
    );
    assert!(
        total_elapsed < TEST_WALL_TIMEOUT,
        "test exceeded {TEST_WALL_TIMEOUT:?} wall clock (actual {total_elapsed:?})"
    );
}

/// Mid-tier gate (between 10k CI and 100k nightly): v3 index, strong cold probed load @ 50k.
#[tokio::test]
#[ignore = "50k v3+cold mid-tier; run: OPENPUFFER_ANN_VERSION=3 cargo test --release -F large_stress --test stress_50k fifty_thousand_docs_v3_cold_probed_validation -- --ignored --nocapture"]
async fn fifty_thousand_docs_v3_cold_probed_validation() {
    let test_started = Instant::now();
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn_with_limits_and_ann_version(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
        Some(10_000),
        None,
        None,
        None,
        None,
        Some(3),
    );
    serve.wait_ready().await;

    let ingest_started = Instant::now();
    ingest_namespace(&serve, NAMESPACE_V3_COLD, STRESS_DOCS, STRESS_BATCH).await;
    let ingest_elapsed = ingest_started.elapsed();

    assert_v3_l0_on_s3(&fixture, NAMESPACE_V3_COLD).await;

    let index_prefix = format!("{ROOT_PREFIX}{NAMESPACE_V3_COLD}/index/");
    let index_keys =
        list_keys_with_prefix(&fixture.client, &fixture.bucket, &index_prefix).await;
    let index_object_count = count_ann_index_objects(&index_keys);
    assert!(
        index_object_count > 0 && index_object_count < 500,
        "v3 object count {index_object_count} should stay under 500 @ 50k"
    );

    let recall = recall_at_10_on_namespace(&serve, NAMESPACE_V3_COLD, STRESS_DOCS).await;

    let mut latencies_ms = Vec::with_capacity(COLD_QUERY_RUNS);
    let mut last_body = json!(null);
    for _ in 0..COLD_QUERY_RUNS {
        let (ms, body) = cold_vector_query_ms(&serve, NAMESPACE_V3_COLD).await;
        latencies_ms.push(ms);
        last_body = body;
    }
    let (p50_query_latency_ms, p90_query_latency_ms, p99_query_latency_ms) =
        latency_percentiles_ms(&latencies_ms);
    let perf = last_body["performance"].as_object().expect("performance");
    let ratio = perf["candidates_ratio"].as_f64().expect("candidates_ratio");
    let roundtrips = perf["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips");
    let cold_keys = perf["cold_s3_keys_fetched"]
        .as_u64()
        .expect("cold_s3_keys_fetched");
    let probed = perf["ann_probed_clusters"].as_u64().unwrap_or(0);

    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE_V3_COLD).await;
    assert_eq!(meta.index_cursor, meta.wal_commit_seq);

    let report = json!({
        "benchmark": "cold_50k_v3",
        "environment": "minio-testcontainers",
        "ann_version": 3,
        "namespace_docs": STRESS_DOCS,
        "dimensions": STRESS_DIM,
        "p50_query_latency_ms": p50_query_latency_ms,
        "p90_query_latency_ms": p90_query_latency_ms,
        "p99_query_latency_ms": p99_query_latency_ms,
        "query_latencies_ms": latencies_ms,
        "cold_query_runs": COLD_QUERY_RUNS,
        "storage_roundtrips": roundtrips,
        "cold_s3_keys_fetched": cold_keys,
        "ann_probed_clusters": probed,
        "candidates_ratio": ratio,
        "recall_at_10": recall,
        "index_object_count": index_object_count,
        "ingest_elapsed_secs": ingest_elapsed.as_secs(),
    });
    println!("{}", serde_json::to_string(&report).expect("bench json"));

    if std::env::var_os("OPENPUFFER_BENCH_WRITE_RESULTS").is_some() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("benchmarks/results/cold-50k-v3.json");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create benchmarks/results");
        }
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&report).expect("pretty json"),
        )
        .expect("write cold-50k-v3.json");
        eprintln!("wrote {}", path.display());
    }

    assert!(
        ratio < MAX_CANDIDATES_RATIO,
        "candidates_ratio {ratio} must be < {MAX_CANDIDATES_RATIO}"
    );
    assert!(
        roundtrips <= MAX_STORAGE_ROUNDTRIPS,
        "storage_roundtrips {roundtrips} must be ≤ {MAX_STORAGE_ROUNDTRIPS}"
    );
    assert!(
        recall >= MIN_RECALL_AT_10_50K,
        "recall@10 {recall} must be ≥ {MIN_RECALL_AT_10_50K}"
    );
    assert!(cold_keys >= 1, "cold path must fetch probed S3 keys");
    assert!(probed >= 1, "v3 cold query must report ann_probed_clusters");

    let total = test_started.elapsed();
    eprintln!(
        "fifty_thousand_docs_v3_cold_probed_validation: ingest={ingest_elapsed:?} total={total:?} ratio={ratio:.4} recall={recall:.4} roundtrips={roundtrips}"
    );
    assert!(
        total < TEST_WALL_TIMEOUT,
        "test exceeded {TEST_WALL_TIMEOUT:?} (actual {total:?})"
    );
}

/// Fast wiring: 2k docs, v3 + cold probed metrics (subset of 50k gate).
#[tokio::test]
#[ignore = "v3+cold wiring; run: cargo test -F large_stress --test stress_50k v3_cold_probed_wiring_at_2k -- --ignored --nocapture"]
async fn v3_cold_probed_wiring_at_2k() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
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

    ingest_namespace(&serve, NAMESPACE_WIRING, WIRING_DOCS, 2_000).await;
    assert_v3_l0_on_s3(&fixture, NAMESPACE_WIRING).await;

    let (_, body) = cold_vector_query_ms(&serve, NAMESPACE_WIRING).await;
    let perf = body["performance"].as_object().expect("performance");
    assert!(
        perf["storage_roundtrips"].as_u64().unwrap_or(99) <= MAX_STORAGE_ROUNDTRIPS,
        "cold storage_roundtrips must be ≤ {MAX_STORAGE_ROUNDTRIPS}"
    );
    assert!(
        perf["cold_s3_keys_fetched"].as_u64().unwrap_or(0) >= 1,
        "cold_s3_keys_fetched required"
    );
    assert!(
        perf["ann_probed_clusters"].as_u64().unwrap_or(0) >= 1,
        "ann_probed_clusters required for v3 cold"
    );
}