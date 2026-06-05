//! Cold-query benchmarks (MinIO testcontainers). Run: `cargo test -F bench --test bench_cold`.
//!
//! Large-dataset comparison program:
//! [`docs/PLAN_LARGE_DATASET_BENCHMARK.md`](../docs/PLAN_LARGE_DATASET_BENCHMARK.md). G2 MinIO gate
//! `bench_cold_10k_synthetic_128_workload_gate` uses the committed **synthetic-128** workload
//! ([`benchmarks/workloads/synthetic-128/`](../benchmarks/workloads/synthetic-128/),
//! `queries.json` cold protocol). Baseline/warm/100k nightly tests use inline stress vectors.
//!
//! `bench_cold_10k_baseline` indexes 10k × 128-dim vectors, runs a cold vector query
//! (`--cache-dir=""`), prints a JSON baseline report, and optionally writes
//! `benchmarks/results/baseline-10k.json` when `OPENPUFFER_BENCH_WRITE_BASELINE=1`.

mod common;

use common::s3_harness::*;
use common::synthetic_workload::{
    cold_query_protocol, load_queries, l1_workload_dir, recall_defaults, resolve_openpuffer_query,
    synthetic_128_schema, upsert_columns_batch,
};
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
const NAMESPACE_SYNTHETIC_128: &str = "bench-synthetic-128-10k";
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

/// Nearest-rank percentile (matches `scripts/bench-large.sh` `percentile_ms`).
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

fn ann_version_from_env() -> Option<u8> {
    std::env::var("OPENPUFFER_ANN_VERSION")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&v| v == 2 || v == 3)
}

fn spawn_bench_serve(
    fixture: &S3Fixture,
    listen: &str,
    cache_dir: Option<PathBuf>,
    write_max_batch_ops: Option<usize>,
    write_max_delay_ms: Option<u64>,
) -> ServeHandle {
    ServeHandle::spawn_with_limits_and_ann_version(
        fixture,
        listen,
        cache_dir,
        write_max_batch_ops,
        write_max_delay_ms,
        None,
        None,
        None,
        ann_version_from_env(),
    )
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
    let query_vec: Vec<f64> = (0..DIM).map(|d| (d as f64 * 0.02).cos()).collect();
    let query = json!({
        "rank_by": ["vector", "ANN", "embedding", query_vec],
        "top_k": 10,
        "consistency": "strong"
    });
    cold_query_with_body_ms(serve, namespace, &query).await
}

async fn cold_query_with_body_ms(
    serve: &ServeHandle,
    namespace: &str,
    query: &Value,
) -> (u64, Value) {
    let client = reqwest::Client::new();
    client
        .post(format!("{}/v1/debug/cache-stats/reset", serve.base_url))
        .send()
        .await
        .expect("cache reset");
    let t0 = Instant::now();
    let resp = client
        .post(format!(
            "{}/v2/namespaces/{}/query",
            serve.base_url,
            namespace_path_segment(namespace)
        ))
        .json(query)
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
    let serve = spawn_bench_serve(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
        Some(10_000),
        None,
    );
    serve.wait_ready().await;
    let ingest_started = Instant::now();
    index_10k_namespace(&fixture, &serve).await;
    let ingest_elapsed = ingest_started.elapsed();

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
    let (p50_query_latency_ms, p90_query_latency_ms, p99_query_latency_ms) =
        latency_percentiles_ms(&latencies_ms);

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
    let ann_version = ann_version_from_env().unwrap_or(2);

    let report = json!({
        "benchmark": "cold_10k",
        "environment": "minio-testcontainers",
        "ann_version": ann_version,
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
        "p90_query_latency_ms": p90_query_latency_ms,
        "p99_query_latency_ms": p99_query_latency_ms,
        "query_latencies_ms": latencies_ms,
        "candidates_ratio": candidates_ratio,
        "index_object_count": index_object_count,
        "index_keys_total": index_keys.len(),
        "cold_query_runs": COLD_QUERY_RUNS,
        "ingest_elapsed_secs": ingest_elapsed.as_secs(),
        "notes": "Probed cold load on MinIO. Regenerate: OPENPUFFER_ANN_VERSION=3 cargo test --release -F bench bench_cold_10k_baseline -- --nocapture"
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
    let serve_cold = spawn_bench_serve(
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
    let serve_warm = spawn_bench_serve(
        &fixture,
        &listen_warm,
        Some(cache_dir.path().to_path_buf()),
        None,
        None,
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

    let mut warm_latencies_ms = Vec::with_capacity(COLD_QUERY_RUNS);
    let mut warm_body = json!(null);
    for _ in 0..COLD_QUERY_RUNS {
        client
            .post(format!(
                "{}/v1/debug/cache-stats/reset",
                serve_warm.base_url
            ))
            .send()
            .await
            .expect("cache reset");
        let t0 = Instant::now();
        let warm_query = client
            .post(format!(
                "{}/v2/namespaces/{}/query",
                serve_warm.base_url,
                namespace_path_segment(NAMESPACE)
            ))
            .json(&json!({
                "rank_by": ["vector", "ANN", "embedding", query_vec.clone()],
                "top_k": 10,
                "consistency": "eventual"
            }))
            .send()
            .await
            .expect("warm query");
        assert_eq!(warm_query.status(), StatusCode::OK);
        warm_latencies_ms.push(t0.elapsed().as_millis() as u64);
        warm_body = warm_query.json().await.expect("warm query json");
    }
    let (p50_warm_query_latency_ms, p90_warm_query_latency_ms, p99_warm_query_latency_ms) =
        latency_percentiles_ms(&warm_latencies_ms);
    let ann_version = ann_version_from_env().unwrap_or(2);
    println!(
        "{}",
        serde_json::to_string(&json!({
            "benchmark": "warm_10k",
            "environment": "minio-testcontainers",
            "ann_version": ann_version,
            "namespace_docs": DOCS,
            "dimensions": DIM,
            "p50_query_latency_ms": p50_warm_query_latency_ms,
            "p90_query_latency_ms": p90_warm_query_latency_ms,
            "p99_query_latency_ms": p99_warm_query_latency_ms,
            "query_latencies_ms": warm_latencies_ms,
            "warm_query_runs": COLD_QUERY_RUNS,
            "notes": "POST /warm + eventual query with disk cache; from bench_cold_10k_warm_vs_cold"
        }))
        .expect("warm json")
    );
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

/// G2 gate: 10k ingest uses synthetic-128 schema; `/recall` + cold query follow `queries.json`.
/// Prints JSON baseline (7 cold samples) for `op-scaling-10k-synthetic128.json`.
#[tokio::test]
async fn bench_cold_10k_synthetic_128_workload_gate() {
    let workload_dir = l1_workload_dir();
    let queries = load_queries(&workload_dir);
    let dim = queries["dim"].as_u64().expect("dim") as usize;
    let (recall_num, recall_top_k) = recall_defaults(&queries);
    let cold_proto = cold_query_protocol(&queries);
    let vector_spec = &queries["vector_queries"][0];
    let cold_query = resolve_openpuffer_query(
        vector_spec.get("openpuffer_query").expect("openpuffer_query"),
        vector_spec.get("vector").expect("vector"),
    );
    assert_eq!(cold_query["top_k"], cold_proto["top_k"]);
    assert_eq!(cold_query["consistency"], cold_proto["consistency"]);

    let fixture = S3Fixture::from_testcontainers().await;
    let listen = format!("127.0.0.1:{}", free_port());
    let serve = spawn_bench_serve(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
        Some(10_000),
        None,
    );
    serve.wait_ready().await;

    let schema = synthetic_128_schema(dim);
    let batches = DOCS / BATCH;
    for b in 0..batches {
        if b > 0 {
            sleep(Duration::from_millis(1100)).await;
        }
        let start = b * BATCH;
        let mut body = json!({ "upsert_columns": upsert_columns_batch(start, BATCH, dim) });
        if b == 0 {
            body["schema"] = schema.clone();
        }
        write_batch(&serve.base_url, NAMESPACE_SYNTHETIC_128, body).await;
    }
    sleep(Duration::from_millis(1200)).await;
    wait_until_indexed(
        &serve.base_url,
        NAMESPACE_SYNTHETIC_128,
        Duration::from_secs(300),
    )
    .await;

    let client = reqwest::Client::new();
    let recall_resp = client
        .post(format!(
            "{}/v1/namespaces/{}/recall",
            serve.base_url,
            namespace_path_segment(NAMESPACE_SYNTHETIC_128)
        ))
        .json(&json!({ "num": recall_num, "top_k": recall_top_k }))
        .send()
        .await
        .expect("recall");
    assert_eq!(recall_resp.status(), StatusCode::OK);
    let recall_body: Value = recall_resp.json().await.expect("recall json");
    let avg_recall = recall_body["avg_recall"].as_f64().expect("avg_recall");
    assert!(
        avg_recall >= 0.85,
        "synthetic-128 workload recall@{} avg_recall {avg_recall} must be >= 0.85",
        recall_top_k
    );

    let mut latencies_ms = Vec::with_capacity(COLD_QUERY_RUNS);
    let mut last_body = json!(null);
    for _ in 0..COLD_QUERY_RUNS {
        let (ms, body) =
            cold_query_with_body_ms(&serve, NAMESPACE_SYNTHETIC_128, &cold_query).await;
        latencies_ms.push(ms);
        last_body = body;
    }
    let (p50_query_latency_ms, p90_query_latency_ms, p99_query_latency_ms) =
        latency_percentiles_ms(&latencies_ms);

    let perf = last_body["performance"].as_object().expect("performance");
    let storage_roundtrips = perf["storage_roundtrips"]
        .as_u64()
        .expect("storage_roundtrips") as u32;
    let ann_version = ann_version_from_env().unwrap_or(2);

    let report = json!({
        "benchmark": "cold_10k_synthetic128",
        "environment": "minio-testcontainers",
        "workload_dir": "benchmarks/workloads/synthetic-128/l1-100k",
        "primary_query": vector_spec.get("name").and_then(|v| v.as_str()).unwrap_or("vector-q00"),
        "ann_version": ann_version,
        "namespace_docs": DOCS,
        "dimensions": dim,
        "cache_dir": "",
        "consistency": "strong",
        "storage_roundtrips": storage_roundtrips,
        "recall_at_10": avg_recall,
        "p50_query_latency_ms": p50_query_latency_ms,
        "p90_query_latency_ms": p90_query_latency_ms,
        "p99_query_latency_ms": p99_query_latency_ms,
        "query_latencies_ms": latencies_ms,
        "cold_query_runs": COLD_QUERY_RUNS,
        "notes": "synthetic-128 G2 gate @ 10k docs; queries.json cold protocol. Regenerate: OPENPUFFER_ANN_VERSION=3 cargo test --release -F bench bench_cold_10k_synthetic_128_workload_gate -- --nocapture"
    });

    println!("{}", serde_json::to_string(&report).expect("synthetic128 json"));

    if std::env::var_os("OPENPUFFER_BENCH_WRITE_BASELINE").is_some() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("benchmarks/results/op-scaling-10k-synthetic128.json");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create benchmarks/results");
        }
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&report).expect("pretty json"),
        )
        .expect("write op-scaling-10k-synthetic128.json");
        eprintln!("wrote {}", path.display());
    }

    assert!(
        storage_roundtrips <= 4,
        "synthetic-128 cold storage_roundtrips {storage_roundtrips} must be ≤ 4"
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
    let serve = spawn_bench_serve(
        &fixture,
        &listen,
        Some(PathBuf::from("")),
        Some(10_000),
        None,
    );
    serve.wait_ready().await;
    let ingest_started = Instant::now();
    index_namespace(
        &fixture,
        &serve,
        NAMESPACE_100K,
        DOCS_100K,
        Duration::from_secs(900),
    )
    .await;
    let ingest_elapsed = ingest_started.elapsed();

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
    let (p50_query_latency_ms, p90_query_latency_ms, p99_query_latency_ms) =
        latency_percentiles_ms(&latencies_ms);

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
    let ann_version = ann_version_from_env().unwrap_or(2);

    let report = json!({
        "benchmark": "cold_100k",
        "environment": "minio-testcontainers",
        "ann_version": ann_version,
        "namespace_docs": DOCS_100K,
        "dimensions": DIM,
        "cache_dir": "",
        "consistency": "strong",
        "index_cursor_eq_wal_commit_seq": true,
        "storage_roundtrips": roundtrips,
        "s3_get_count": s3_get_count,
        "p50_query_latency_ms": p50_query_latency_ms,
        "p90_query_latency_ms": p90_query_latency_ms,
        "p99_query_latency_ms": p99_query_latency_ms,
        "query_latencies_ms": latencies_ms,
        "candidates_ratio": ratio,
        "recall_at_10": recall,
        "index_object_count": index_object_count,
        "index_keys_total": index_keys.len(),
        "cold_query_runs": COLD_QUERY_RUNS,
        "ingest_elapsed_secs": ingest_elapsed.as_secs(),
        "notes": "Nightly 100k gate. Regenerate: OPENPUFFER_ANN_VERSION=3 cargo test --release -F bench bench_cold_100k_nightly -- --ignored --nocapture"
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

/// 100k warm path for op-scaling tier (ingest+index then `POST …/warm` + eventual queries).
#[tokio::test]
#[ignore = "100k MinIO ingest + index (~3–8 min); run: cargo test -F bench bench_cold_100k_warm -- --ignored --nocapture"]
async fn bench_cold_100k_warm() {
    let fixture = S3Fixture::from_testcontainers().await;
    let cache_dir = tempfile::tempdir().expect("warm cache tempdir");
    let listen = format!("127.0.0.1:{}", free_port());
    // Single serve with disk cache (same process for ingest + POST /warm avoids WAL read races).
    let serve = spawn_bench_serve(
        &fixture,
        &listen,
        Some(cache_dir.path().to_path_buf()),
        Some(10_000),
        None,
    );
    serve.wait_ready().await;
    let ingest_started = Instant::now();
    index_namespace(
        &fixture,
        &serve,
        NAMESPACE_100K,
        DOCS_100K,
        Duration::from_secs(900),
    )
    .await;
    let ingest_elapsed = ingest_started.elapsed();
    // Let WAL segments settle before warm prefetch (avoids transient read wal NNN on MinIO).
    sleep(Duration::from_secs(2)).await;

    let query_vec: Vec<f64> = (0..DIM).map(|d| (d as f64 * 0.02).cos()).collect();
    let client = reqwest::Client::new();

    let warm_url = format!(
        "{}/v1/namespaces/{}/warm",
        serve.base_url,
        namespace_path_segment(NAMESPACE_100K)
    );
    let mut warm_pin = json!(null);
    let mut warm_ok = false;
    for attempt in 0..5 {
        if attempt > 0 {
            sleep(Duration::from_millis(500 * attempt as u64)).await;
        }
        let warm_resp = client
            .post(&warm_url)
            .send()
            .await
            .expect("warm request");
        let warm_status = warm_resp.status();
        warm_pin = warm_resp.json().await.expect("warm json");
        if warm_status == StatusCode::OK {
            warm_ok = true;
            break;
        }
    }
    assert!(
        warm_ok,
        "warm failed after retries: {}",
        serde_json::to_string(&warm_pin).unwrap_or_default()
    );
    assert_eq!(warm_pin["status"], "ok");
    assert!(warm_pin["pinned"].as_bool().unwrap_or(false));

    let mut warm_latencies_ms = Vec::with_capacity(COLD_QUERY_RUNS);
    let mut warm_body = json!(null);
    for _ in 0..COLD_QUERY_RUNS {
        client
            .post(format!(
                "{}/v1/debug/cache-stats/reset",
                serve.base_url
            ))
            .send()
            .await
            .expect("cache reset");
        let t0 = Instant::now();
        let warm_query = client
            .post(format!(
                "{}/v2/namespaces/{}/query",
                serve.base_url,
                namespace_path_segment(NAMESPACE_100K)
            ))
            .json(&json!({
                "rank_by": ["vector", "ANN", "embedding", query_vec.clone()],
                "top_k": 10,
                "consistency": "eventual"
            }))
            .send()
            .await
            .expect("warm query");
        assert_eq!(warm_query.status(), StatusCode::OK);
        warm_latencies_ms.push(t0.elapsed().as_millis() as u64);
        warm_body = warm_query.json().await.expect("warm query json");
    }
    let (p50_warm_query_latency_ms, p90_warm_query_latency_ms, p99_warm_query_latency_ms) =
        latency_percentiles_ms(&warm_latencies_ms);
    let ann_version = ann_version_from_env().unwrap_or(2);

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
        .get(format!("{}/v1/debug/cache-stats", serve.base_url))
        .send()
        .await
        .expect("cache stats")
        .json()
        .await
        .expect("stats json");
    let s3_gets = stats["s3_get_count"].as_u64().unwrap_or(0);
    assert!(
        s3_gets <= 2,
        "warm queries should not refetch index segments (s3_get_count={s3_gets})"
    );

    println!(
        "{}",
        serde_json::to_string(&json!({
            "benchmark": "warm_100k",
            "environment": "minio-testcontainers",
            "ann_version": ann_version,
            "namespace_docs": DOCS_100K,
            "dimensions": DIM,
            "p50_query_latency_ms": p50_warm_query_latency_ms,
            "p90_query_latency_ms": p90_warm_query_latency_ms,
            "p99_query_latency_ms": p99_warm_query_latency_ms,
            "query_latencies_ms": warm_latencies_ms,
            "warm_query_runs": COLD_QUERY_RUNS,
            "ingest_elapsed_secs": ingest_elapsed.as_secs(),
            "notes": "POST /warm + eventual query with disk cache; from bench_cold_100k_warm"
        }))
        .expect("warm 100k json")
    );
}