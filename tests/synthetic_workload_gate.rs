//! G2 fixture gates: synthetic-128 manifest/queries consistency (no Docker).
//!
//! Run: `cargo test --test synthetic_workload_gate`

mod common;

use common::synthetic_workload::*;

#[test]
fn synthetic_128_l1_fixture_vectors_match_bench_sin_v1() {
    let dir = l1_workload_dir();
    let manifest = load_manifest(&dir);
    let queries = load_queries(&dir);
    assert_manifest_queries_consistent(&manifest, &queries);
    let dim = manifest["dim"].as_u64().expect("dim") as usize;
    assert_eq!(manifest["embedding_fn"].as_str(), Some("bench_sin_v1"));
    // f64 sin pipeline matches Python float output within ~1e-12 on L1 fixture.
    assert_queries_vectors_match_bench_sin(&queries, dim, 1e-9);
}

#[test]
fn synthetic_128_l1_recall_defaults_match_plan() {
    let queries = load_queries(&l1_workload_dir());
    let (num, top_k) = recall_defaults(&queries);
    assert_eq!(num, 20, "recall_defaults.num (plan §3.2)");
    assert_eq!(top_k, 10, "recall_defaults.top_k");
    let cold = cold_query_protocol(&queries);
    assert_eq!(cold["top_k"].as_u64(), Some(10));
    assert_eq!(cold["consistency"].as_str(), Some("strong"));
    assert_eq!(cold["runs"].as_u64(), Some(7));
}

#[test]
fn synthetic_128_l1_filter_hybrid_query_sets_complete() {
    let queries = load_queries(&l1_workload_dir());
    assert_workload_filter_hybrid_counts(&queries);
    for (i, spec) in filter_query_specs(&queries).iter().enumerate() {
        assert!(
            spec.get("openpuffer_query").is_some() && spec.get("vector").is_some(),
            "filter_queries[{i}] must have openpuffer_query and vector"
        );
        assert!(spec.get("name").is_some(), "filter_queries[{i}].name");
    }
    for (i, spec) in hybrid_query_specs(&queries).iter().enumerate() {
        assert!(
            spec.get("openpuffer_query").is_some() && spec.get("vector").is_some(),
            "hybrid_queries[{i}] must have openpuffer_query and vector"
        );
        assert!(spec.get("name").is_some(), "hybrid_queries[{i}].name");
    }
}