//! Performance / regression tests (optional: `cargo test --features perf`).
//!
//! Builds a 5k-doc namespace with 128-dim vectors, runs indexed ANN queries, and asserts
//! candidate generation stays sub-linear (not O(n)).

use openpuffer::index::vector::VectorIndex;
use openpuffer::meta::{DistanceMetric, NamespaceMeta};
use openpuffer::models::{Document, QueryRequest};
use openpuffer::search::{execute_query, QueryConsistency, QueryContext};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

const N: usize = 5_000;
const DIM: usize = 128;
const QUERY_RUNS: usize = 20;
/// 8-probe ANN on 5k docs is ~(8/√n) ≈ 10–11%; 12% catches O(n) regressions.
const MAX_CANDIDATE_RATIO: f64 = 0.12;
/// Warm in-memory query p50 ceiling (dev machines; not a hard SLA).
const MAX_P50_MS: u64 = 500;

fn build_namespace() -> (HashMap<String, Document>, VectorIndex, NamespaceMeta) {
    let mut map = HashMap::with_capacity(N);
    let mut pairs = Vec::with_capacity(N);
    for i in 0..N {
        let id = format!("doc-{i}");
        let embedding: Vec<f64> = (0..DIM)
            .map(|d| ((i * DIM + d) as f64 * 0.001).sin())
            .collect();
        let doc = Document {
            id: id.clone(),
            attributes: [
                ("text".into(), json!(format!("lorem ipsum document number {i}"))),
                ("embedding".into(), json!(embedding)),
            ]
            .into(),
        };
        pairs.push((id, doc.clone()));
        map.insert(doc.id.clone(), doc);
    }
    let vindex = VectorIndex::build(1, "embedding", DistanceMetric::CosineDistance, &pairs)
        .expect("build vector index")
        .expect("vector index present");
    let meta = NamespaceMeta {
        index_cursor: 1,
        wal_commit_seq: 1,
        vector_segment_id: 1,
        dimensions: DIM as u32,
        ..Default::default()
    };
    (map, vindex, meta)
}

fn p50_ms(samples: &mut [u64]) -> u64 {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

#[test]
fn perf_indexed_vector_query_candidate_ratio_and_latency() {
    let (map, vindex, meta) = build_namespace();
    let tail = HashSet::new();
    let ctx = QueryContext {
        docs: &map,
        meta: &meta,
        fts: None,
        vector: Some(&vindex),
        filter_index: None,
        tail_doc_ids: &tail,
        consistency: QueryConsistency::Strong,
        storage_roundtrips: None,
    };
    let query_vec: Vec<f64> = (0..DIM).map(|d| (d as f64 * 0.02).cos()).collect();
    let req = QueryRequest {
        rank_by: json!(["vector", "ANN", "embedding", query_vec]),
        top_k: Some(10),
        filters: None,
        include_attributes: None,
        consistency: None,
        order_by: None,
    };

    // Warm planner + index structures.
    let warm = execute_query(&ctx, &req).expect("warm query");
    let warm_perf = warm.performance.expect("performance");
    assert_eq!(warm_perf.approx_namespace_size, N as u64);
    assert!(
        warm_perf.candidates_ratio < MAX_CANDIDATE_RATIO,
        "warm query scanned too many docs: candidates={} ratio={}",
        warm_perf.candidates,
        warm_perf.candidates_ratio
    );

    let mut latencies_us = Vec::with_capacity(QUERY_RUNS);
    for _ in 0..QUERY_RUNS {
        let t0 = Instant::now();
        let resp = execute_query(&ctx, &req).expect("query");
        latencies_us.push(t0.elapsed().as_micros() as u64);
        let perf = resp.performance.expect("performance");
        assert!(
            perf.candidates_ratio < MAX_CANDIDATE_RATIO,
            "run exceeded candidate ratio: candidates={} ratio={}",
            perf.candidates,
            perf.candidates_ratio
        );
        assert_eq!(perf.exhaustive_search_count, 0);
    }

    let p50 = p50_ms(
        &mut latencies_us
            .iter()
            .map(|us| us / 1000)
            .collect::<Vec<_>>(),
    );
    assert!(
        p50 < MAX_P50_MS,
        "p50 query latency {p50}ms exceeds {MAX_P50_MS}ms (warm in-memory)"
    );
}