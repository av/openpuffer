//! Cold-query benchmarks (MinIO testcontainers). Run: `cargo test -F bench --test bench_cold`.
//!
//! `bench_cold_10k_baseline` indexes 10k × 128-dim vectors, runs a cold vector query
//! (`--cache-dir=""`), prints a JSON baseline report, and optionally writes
//! `benchmarks/results/baseline-10k.json` when `OPENPUFFER_BENCH_WRITE_BASELINE=1`.

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

const NAMESPACE: &str = "bench-cold-10k";
const NAMESPACE_100K: &str = "bench-cold-100k";
const DOCS: usize = 10_000;
const DOCS_100K: usize = 100_000;
const BATCH: usize = 2_000;
const DIM: usize = 128;
const COLD_QUERY_RUNS: usize = 7;
const RECALL_QUERIES_100K: usize = 20;
const RECALL_TOP_K: usize = 10;

fn stress_upsert_columns(start: usize, count: usize) -> Value {
    let mut ids = Vec::with_capacity(count);
    let mut texts = Vec::with_capacity(count);
    let mut embeddings = Vec::with_capacity(count);
    for i in start..start + count {
        ids.push(json!(format!("doc-{i}")));
        texts.push(json!(format!("stressterm document number {i}")));
        let emb: Vec<f64> = (0..DIM)
            .map(|d| ((i * DIM + d) as f64 * 0.001).sin())
            .collect();
        embeddings.push(json!(emb));
    }
    json!({
        "id": ids,
        "text": texts,
        "embedding": embeddings
    })
}

fn count_ann_index_objects(keys: &[String]) -> usize {
    keys.iter()
        .filter(|k| {
            k.contains("clusters-") || (k.contains("centroids-l1-") && k.ends_with(".bin"))
        })
        .count()
}

fn p50_ms(samples: &mut [u64]) -> u64 {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn synthetic_embedding(doc_index: usize) -> Vec<f64> {
    (0..DIM)
        .map(|d| ((doc_index * DIM + d) as f64 * 0.001).sin())
        .collect()
}

async fn index_namespace(
    fixture: &S3Fixture,
    serve: &ServeHandle,
    namespace: &str,
    docs: usize,
    index_timeout: Duration,
) {
    let schema = json!({
        "text": {"type": "string", "full_text_search": true},
        "embedding": "[128]f32"
    });
    let batches = docs / BATCH;
    assert_eq!(batches * BATCH, docs);
    for b in 0..batches {
        if b > 0 {
            sleep(Duration::from_millis(1100)).await;
        }
        let start = b * BATCH;
        let mut body = json!({ "upsert_columns": stress_upsert_columns(start, BATCH) });
        if b == 0 {
            body["schema"] = schema.clone();
        }
        write_batch(&serve.base_url, namespace, body).await;
    }
    sleep(Duration::from_millis(1200)).await;
    wait_until_indexed(&serve.base_url, namespace, index_timeout).await;
    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, namespace).await;
    assert_eq!(
        meta.index_cursor, meta.wal_commit_seq,
        "namespace must be fully indexed before cold bench"
    );
}

async fn index_10k_namespace(fixture: &S3Fixture, serve: &ServeHandle) {
    index_namespace(fixture, serve, NAMESPACE, DOCS, Duration::from_secs(300)).await;
}

async fn recall_at_10_on_namespace(serve: &ServeHandle, namespace: &str, docs: usize) -> f64 {
    let mut vectors: Vec<(String, Vec<f64>)> = Vec::with_capacity(docs);
    for i in 0..docs {
        vectors.push((format!("doc-{i}"), synthetic_embedding(i)));
    }
    let metric = DistanceMetric::CosineDistance;
    let client = reqwest::Client::new();
    let mut recall_sum = 0.0f64;
    for q in 0..RECALL_QUERIES_100K {
        let query = vectors[q * (docs / RECALL_QUERIES_100K)].1.clone();
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
    recall_sum / RECALL_QUERIES_100K as f64
}

async fn cold_vector_query_ms(serve: &ServeHandle, namespace: &str) -> (u64, Value) {
    let client = reqwest::Client::new();
    client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");
    let query_vec: Vec<f64> = (0..DIM).map(|d| (d as f64 * 0.02).cos()).collect();
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
    let body: Value = resp.json().await.expect("query json");
    (ms, body)
}

/// Records pre–Phase-A cold-query baseline on MinIO (not an SLO gate).
#[tokio::test]
async fn bench_cold_10k_baseline() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn_with_options(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
        Some(10_000),
        None,
    );
    serve.wait_ready().await;
    index_10k_namespace(&fixture, &serve).await;

    let index_prefix = format!("{ROOT_PREFIX}{NAMESPACE}/index/");
    let index_keys =
        list_keys_with_prefix(&fixture.client, &fixture.bucket, &index_prefix).await;
    let index_object_count = count_ann_index_objects(&index_keys);

    let mut latencies_ms = Vec::with_capacity(COLD_QUERY_RUNS);
    let mut last_body = json!(null);
    for _ in 0..COLD_QUERY_RUNS {
        let (ms, body) = cold_vector_query_ms(&serve, NAMESPACE).await;
        latencies_ms.push(ms);
        last_body = body;
    }
    let p50_query_latency_ms = p50_ms(&mut latencies_ms);

    let perf = last_body["performance"].as_object().expect("performance");
    let storage_roundtrips = perf["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips") as u32;
    let cold_s3_keys_fetched = perf["cold_s3_keys_fetched"]
        .as_u64()
        .expect("cold_s3_keys_fetched") as u32;
    let candidates_ratio = perf["candidates_ratio"].as_f64().expect("candidates_ratio");

    let client = reqwest::Client::new();
    let stats: Value = client
        .get(format!("{}/v1/debug/cache-stats", serve.base_url))
        .send()
        .await
        .expect("cache stats")
        .json()
        .await
        .expect("stats json");
    let s3_get_count = stats["s3_get_count"].as_u64().expect("s3_get_count");

    let report = json!({
        "benchmark": "cold_10k",
        "environment": "minio-testcontainers",
        "namespace_docs": DOCS,
        "dimensions": DIM,
        "cache_dir": "",
        "consistency": "strong",
        "index_cursor_eq_wal_commit_seq": true,
        "storage_roundtrips": storage_roundtrips,
        "cold_s3_keys_fetched": cold_s3_keys_fetched,
        "s3_get_count": s3_get_count,
        "s3_get_count_note": "segment cache counter; cold path uses s3_batch (see cold_s3_keys_fetched)",
        "p50_query_latency_ms": p50_query_latency_ms,
        "candidates_ratio": candidates_ratio,
        "index_object_count": index_object_count,
        "index_keys_total": index_keys.len(),
        "cold_query_runs": COLD_QUERY_RUNS,
        "notes": "Post-Phase-A probed cold load (v2 index, 2026-06-03 MinIO). storage_roundtrips=2; cluster GETs bounded by probe plan (index_object_count on disk ≠ cold_s3_keys_fetched). s3_get_count=0 expected (cold uses s3_batch). Regenerate: OPENPUFFER_BENCH_WRITE_BASELINE=1 cargo test -F bench bench_cold_10k_baseline -- --nocapture"
    });

    println!("{}", serde_json::to_string(&report).expect("baseline json"));

    if std::env::var_os("OPENPUFFER_BENCH_WRITE_BASELINE").is_some() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("benchmarks/results/baseline-10k.json");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create benchmarks/results");
        }
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&report).expect("pretty json"),
        )
        .expect("write baseline-10k.json");
        eprintln!("wrote {}", path.display());
    }

    assert!(
        candidates_ratio < 0.20,
        "candidates_ratio {candidates_ratio} should stay sub-linear on 10k"
    );
    assert!(storage_roundtrips >= 2, "cold path should report batched roundtrips");
    assert!(
        cold_s3_keys_fetched >= 1,
        "cold path should report cold_s3_keys_fetched in performance JSON"
    );
    assert!(index_object_count > 0, "expected ANN index objects on S3");
}

/// Same 10k ANN query: cold (`--cache-dir=""`) reports probed S3 metrics; warm (`POST …/warm`) hits disk cache.
#[tokio::test]
async fn bench_cold_10k_warm_vs_cold() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen_cold = format!("127.0.0.1:{}", free_port());
    let serve_cold = ServeHandle::spawn_with_options(
        &fixture,
        &listen_cold,
        Some(PathBuf::from("")),
        Some(10_000),
        None,
    );
    serve_cold.wait_ready().await;
    index_10k_namespace(&fixture, &serve_cold).await;

    let query_vec: Vec<f64> = (0..DIM).map(|d| (d as f64 * 0.02).cos()).collect();
    let client = reqwest::Client::new();

    // Cold eventual first (before view is pinned) — probed index keys, no WAL tail work.
    client
        .post(format!(
            "{}/v1/debug/cache-stats/reset",
            serve_cold.base_url
        ))
        .send()
        .await
        .expect("cache reset");
    let eventual_resp = client
        .post(format!(
            "{}/v2/namespaces/{}/query",
            serve_cold.base_url,
            namespace_path_segment(NAMESPACE)
        ))
        .json(&json!({
            "rank_by": ["vector", "ANN", "embedding", query_vec.clone()],
            "top_k": 10,
            "consistency": "eventual"
        }))
        .send()
        .await
        .expect("cold eventual query");
    assert_eq!(eventual_resp.status(), StatusCode::OK);
    let eventual_body: Value = eventual_resp.json().await.expect("eventual json");
    let ev_perf = eventual_body["performance"].as_object().expect("performance");
    let ev_roundtrips = ev_perf["storage_roundtrips"]
        .as_u64()
        .expect("eventual storage_roundtrips");
    let ev_cold_keys = ev_perf["cold_s3_keys_fetched"]
        .as_u64()
        .expect("eventual cold_s3_keys_fetched");
    let ev_exhaustive = ev_perf["exhaustive_search_count"].as_u64().unwrap_or(0);
    assert_eq!(
        ev_exhaustive, 0,
        "eventual cold on caught-up namespace must not score unindexed WAL tail"
    );
    assert!(
        ev_cold_keys >= 1,
        "eventual cold must still report probed cold_s3_keys_fetched, got {ev_cold_keys}"
    );
    assert!(
        ev_roundtrips >= 2,
        "eventual cold: meta + bootstrap + probe, got {ev_roundtrips}"
    );

    // Cold strong on pinned view — same probed metrics, roundtrips >= eventual.
    client
        .post(format!(
            "{}/v1/debug/cache-stats/reset",
            serve_cold.base_url
        ))
        .send()
        .await
        .expect("cache reset");
    let (_, strong_body) = cold_vector_query_ms(&serve_cold, NAMESPACE).await;
    let st_perf = strong_body["performance"].as_object().expect("performance");
    let st_roundtrips = st_perf["storage_roundtrips"]
        .as_u64()
        .expect("strong storage_roundtrips");
    let st_cold_keys = st_perf["cold_s3_keys_fetched"]
        .as_u64()
        .expect("strong cold_s3_keys_fetched");
    assert!(
        st_cold_keys >= 1,
        "strong cold must report cold_s3_keys_fetched, got {st_cold_keys}"
    );
    assert!(
        st_roundtrips >= ev_roundtrips,
        "strong cold roundtrips {st_roundtrips} must be >= eventual {ev_roundtrips}"
    );
    assert!(
        st_cold_keys >= ev_cold_keys,
        "strong cold keys {st_cold_keys} should be >= eventual {ev_cold_keys} on first strong pass"
    );

    // Warm path: disk cache + warm pin — no cold S3 batch metrics, zero segment GETs.
    let cache_dir = tempfile::tempdir().expect("warm cache tempdir");
    let listen_warm = format!("127.0.0.1:{}", free_port());
    let serve_warm = ServeHandle::spawn_with_cache(
        &fixture,
        &listen_warm,
        Some(cache_dir.path().to_path_buf()),
    );
    serve_warm.wait_ready().await;

    let warm_resp = client
        .post(format!(
            "{}/v1/namespaces/{}/warm",
            serve_warm.base_url,
            namespace_path_segment(NAMESPACE)
        ))
        .send()
        .await
        .expect("warm request");
    assert_eq!(warm_resp.status(), StatusCode::OK);
    let warm_body: Value = warm_resp.json().await.expect("warm json");
    assert_eq!(warm_body["status"], "ok");
    assert!(warm_body["pinned"].as_bool().unwrap_or(false));

    client
        .post(format!(
            "{}/v1/debug/cache-stats/reset",
            serve_warm.base_url
        ))
        .send()
        .await
        .expect("cache reset");
    let warm_query = client
        .post(format!(
            "{}/v2/namespaces/{}/query",
            serve_warm.base_url,
            namespace_path_segment(NAMESPACE)
        ))
        .json(&json!({
            "rank_by": ["vector", "ANN", "embedding", query_vec],
            "top_k": 10,
            "consistency": "eventual"
        }))
        .send()
        .await
        .expect("warm query");
    assert_eq!(warm_query.status(), StatusCode::OK);
    let warm_body: Value = warm_query.json().await.expect("warm query json");
    let warm_perf = warm_body["performance"].as_object().expect("performance");
    assert!(
        warm_perf.get("storage_roundtrips").is_none()
            || warm_perf["storage_roundtrips"].is_null(),
        "warm query must not report cold storage_roundtrips: {warm_perf:?}"
    );
    let warm_cold_keys = warm_perf
        .get("cold_s3_keys_fetched")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    assert_eq!(
        warm_cold_keys, 0,
        "warm path must not increment cold_s3_keys_fetched"
    );

    let stats: Value = client
        .get(format!("{}/v1/debug/cache-stats", serve_warm.base_url))
        .send()
        .await
        .expect("cache stats")
        .json()
        .await
        .expect("stats json");
    assert_eq!(
        stats["s3_get_count"].as_u64(),
        Some(0),
        "warm + eventual query should not S3 GetObject index segments (disk cache hit)"
    );

    assert!(
        st_cold_keys > warm_cold_keys,
        "cold strong cold_s3_keys_fetched ({st_cold_keys}) must exceed warm ({warm_cold_keys})"
    );
}

/// Phase A gate: strong caught-up cold query must use ≤4 storage roundtrips.
#[tokio::test]
async fn bench_cold_10k_storage_roundtrips_at_most_four() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn_with_cache(&fixture, &listen, Some(PathBuf::from("")));
    serve.wait_ready().await;
    index_10k_namespace(&fixture, &serve).await;

    let (_, body) = cold_vector_query_ms(&serve, NAMESPACE).await;
    let roundtrips = body["performance"]["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips");
    assert!(
        roundtrips <= 4,
        "storage_roundtrips {roundtrips} must be ≤ 4 for caught-up strong cold query"
    );
}

/// Nightly scale gate: 100k indexed namespace, cold ANN recall and candidate fraction.
#[tokio::test]
#[ignore = "100k MinIO ingest + index (~15–30 min); run in nightly: cargo test -F bench bench_cold_100k_nightly -- --ignored --nocapture"]
async fn bench_cold_100k_nightly() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn_with_options(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
        Some(10_000),
        None,
    );
    serve.wait_ready().await;
    index_namespace(
        &fixture,
        &serve,
        NAMESPACE_100K,
        DOCS_100K,
        Duration::from_secs(900),
    )
    .await;

    let index_prefix = format!("{ROOT_PREFIX}{NAMESPACE_100K}/index/");
    let index_keys =
        list_keys_with_prefix(&fixture.client, &fixture.bucket, &index_prefix).await;
    let index_object_count = count_ann_index_objects(&index_keys);

    let recall = recall_at_10_on_namespace(&serve, NAMESPACE_100K, DOCS_100K).await;

    let mut latencies_ms = Vec::with_capacity(COLD_QUERY_RUNS);
    let mut last_body = json!(null);
    for _ in 0..COLD_QUERY_RUNS {
        let (ms, body) = cold_vector_query_ms(&serve, NAMESPACE_100K).await;
        latencies_ms.push(ms);
        last_body = body;
    }
    let p50_query_latency_ms = p50_ms(&mut latencies_ms);

    let perf = last_body["performance"].as_object().expect("performance");
    let ratio = perf["candidates_ratio"].as_f64().expect("candidates_ratio");
    let roundtrips = perf["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips");

    let client = reqwest::Client::new();
    let stats: Value = client
        .get(format!("{}/v1/debug/cache-stats", serve.base_url))
        .send()
        .await
        .expect("cache stats")
        .json()
        .await
        .expect("stats json");
    let s3_get_count = stats["s3_get_count"].as_u64().expect("s3_get_count");

    let report = json!({
        "benchmark": "cold_100k",
        "environment": "minio-testcontainers",
        "namespace_docs": DOCS_100K,
        "dimensions": DIM,
        "cache_dir": "",
        "consistency": "strong",
        "index_cursor_eq_wal_commit_seq": true,
        "storage_roundtrips": roundtrips,
        "s3_get_count": s3_get_count,
        "p50_query_latency_ms": p50_query_latency_ms,
        "candidates_ratio": ratio,
        "recall_at_10": recall,
        "index_object_count": index_object_count,
        "index_keys_total": index_keys.len(),
        "cold_query_runs": COLD_QUERY_RUNS,
        "notes": "Nightly 100k gate. Regenerate: cargo test -F bench bench_cold_100k_nightly -- --ignored --nocapture"
    });
    println!("{}", serde_json::to_string(&report).expect("bench json"));

    if std::env::var_os("OPENPUFFER_BENCH_WRITE_RESULTS").is_some() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("benchmarks/results/nightly-100k.json");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create benchmarks/results");
        }
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&report).expect("pretty json"),
        )
        .expect("write nightly-100k.json");
        eprintln!("wrote {}", path.display());
    }

    assert!(
        recall >= 0.88,
        "recall@10 {recall} must be ≥ 0.88 on 100k synthetic"
    );
    assert!(
        ratio < 0.20,
        "candidates_ratio {ratio} must be < 0.20 on 100k"
    );
    assert!(
        roundtrips <= 4,
        "storage_roundtrips {roundtrips} must be ≤ 4"
    );
}