//! Cold-query benchmarks (MinIO testcontainers). Run: `cargo test -F bench --test bench_cold`.
//!
//! `bench_cold_10k_baseline` indexes 10k × 128-dim vectors, runs a cold vector query
//! (`--cache-dir=""`), prints a JSON baseline report, and optionally writes
//! `benchmarks/results/baseline-10k.json` when `OPENPUFFER_BENCH_WRITE_BASELINE=1`.

mod common;

use common::s3_harness::*;
use openpuffer::models::ROOT_PREFIX;
use reqwest::StatusCode;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::time::sleep;

const NAMESPACE: &str = "bench-cold-10k";
const DOCS: usize = 10_000;
const BATCH: usize = 2_000;
const DIM: usize = 128;
const COLD_QUERY_RUNS: usize = 7;

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

async fn index_10k_namespace(fixture: &S3Fixture, serve: &ServeHandle) {
    let schema = json!({
        "text": {"type": "string", "full_text_search": true},
        "embedding": "[128]f32"
    });
    let batches = DOCS / BATCH;
    assert_eq!(batches * BATCH, DOCS);
    for b in 0..batches {
        if b > 0 {
            sleep(Duration::from_millis(1100)).await;
        }
        let start = b * BATCH;
        let mut body = json!({ "upsert_columns": stress_upsert_columns(start, BATCH) });
        if b == 0 {
            body["schema"] = schema.clone();
        }
        write_batch(&serve.base_url, NAMESPACE, body).await;
    }
    sleep(Duration::from_millis(1200)).await;
    wait_until_indexed(&serve.base_url, NAMESPACE, Duration::from_secs(300)).await;
    let meta = fetch_meta_from_s3(&fixture.client, &fixture.bucket, NAMESPACE).await;
    assert_eq!(
        meta.index_cursor, meta.wal_commit_seq,
        "namespace must be fully indexed before cold bench"
    );
}

async fn cold_vector_query_ms(serve: &ServeHandle) -> (u64, Value) {
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
            namespace_path_segment(NAMESPACE)
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
        let (ms, body) = cold_vector_query_ms(&serve).await;
        latencies_ms.push(ms);
        last_body = body;
    }
    let p50_query_latency_ms = p50_ms(&mut latencies_ms);

    let perf = last_body["performance"].as_object().expect("performance");
    let storage_roundtrips = perf["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips") as u32;
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
        "s3_get_count": s3_get_count,
        "p50_query_latency_ms": p50_query_latency_ms,
        "candidates_ratio": candidates_ratio,
        "index_object_count": index_object_count,
        "index_keys_total": index_keys.len(),
        "cold_query_runs": COLD_QUERY_RUNS,
        "notes": "Pre-Phase-A baseline; cold load fetches full index. Regenerate: OPENPUFFER_BENCH_WRITE_BASELINE=1 cargo test -F bench bench_cold_10k_baseline -- --nocapture"
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
    assert!(index_object_count > 0, "expected ANN index objects on S3");
}

/// Phase A gate: strong caught-up cold query must use ≤4 storage roundtrips (not met pre–Phase A).
#[tokio::test]
#[ignore = "Phase A: storage_roundtrips ≤ 4 on 10k cold query"]
async fn bench_cold_10k_storage_roundtrips_at_most_four() {
    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = ServeHandle::spawn_with_cache(&fixture, &listen, Some(PathBuf::from("")));
    serve.wait_ready().await;
    index_10k_namespace(&fixture, &serve).await;

    let (_, body) = cold_vector_query_ms(&serve).await;
    let roundtrips = body["performance"]["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips");
    assert!(
        roundtrips <= 4,
        "storage_roundtrips {roundtrips} must be ≤ 4 for caught-up strong cold query"
    );
}