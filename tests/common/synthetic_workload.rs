//! Shared **synthetic-128** workload helpers (G2 correctness gates).
//!
//! Loads committed `manifest.json` / `queries.json` under `benchmarks/workloads/synthetic-128/`
//! and generates ingest/query bodies matching `benchmarks/workloads/generate_synthetic.py`.

use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub const L1_WORKLOAD_DIR: &str = "benchmarks/workloads/synthetic-128/l1-100k";

/// Fixed counts from `generate_synthetic.py` defaults (PLAN §1.2 query set).
pub const EXPECTED_FILTER_QUERY_COUNT: usize = 6;
pub const EXPECTED_HYBRID_QUERY_COUNT: usize = 4;

const CATEGORIES: [&str; 8] = [
    "cat-0", "cat-1", "cat-2", "cat-3", "cat-4", "cat-5", "cat-6", "cat-7",
];

/// Default L1 tier directory (manifest + queries committed in-repo).
pub fn l1_workload_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(L1_WORKLOAD_DIR)
}

pub fn bench_sin_embedding(doc_index: usize, dim: usize) -> Vec<f64> {
    (0..dim)
        .map(|d| ((doc_index * dim + d) as f64 * 0.001).sin())
        .collect()
}

pub fn bench_sin_embedding_json(doc_index: usize, dim: usize) -> Value {
    Value::Array(
        bench_sin_embedding(doc_index, dim)
            .into_iter()
            .map(|v| json!(v))
            .collect(),
    )
}

pub fn load_manifest(dir: &Path) -> Value {
    let path = dir.join("manifest.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

pub fn load_queries(dir: &Path) -> Value {
    let path = dir.join("queries.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

/// openpuffer schema for synthetic-128 (category/priority filters + FTS on `text`).
pub fn synthetic_128_schema(dim: usize) -> Value {
    json!({
        "text": {"type": "string", "full_text_search": true},
        "embedding": format!("[{dim}]f32"),
        "category": {"type": "string", "filterable": true},
        "title": {"type": "string"},
        "priority": {"type": "int", "filterable": true}
    })
}

/// Columnar upsert matching `generate_synthetic.py` (`bench_sin_v1`, doc-prefix ids).
pub fn upsert_columns_batch(start: usize, count: usize, dim: usize) -> Value {
    let mut ids = Vec::with_capacity(count);
    let mut embeddings = Vec::with_capacity(count);
    let mut categories = Vec::with_capacity(count);
    let mut titles = Vec::with_capacity(count);
    let mut priorities = Vec::with_capacity(count);
    let mut texts = Vec::with_capacity(count);
    for i in start..start + count {
        ids.push(json!(format!("doc-{i}")));
        embeddings.push(bench_sin_embedding_json(i, dim));
        categories.push(json!(CATEGORIES[i % CATEGORIES.len()]));
        titles.push(json!(format!("synthetic title {i}")));
        priorities.push(json!(i % 100));
        texts.push(json!(format!("stressterm document number {i}")));
    }
    json!({
        "id": ids,
        "embedding": embeddings,
        "category": categories,
        "title": titles,
        "priority": priorities,
        "text": texts
    })
}

/// Substitute `"$vector"` placeholders in workload `openpuffer_query` specs.
pub fn resolve_openpuffer_query(template: &Value, vector: &Value) -> Value {
    inject_vector_placeholder(template.clone(), vector)
}

fn inject_vector_placeholder(value: Value, vector: &Value) -> Value {
    match value {
        Value::String(s) if s == "$vector" => vector.clone(),
        Value::Array(arr) => Value::Array(
            arr.into_iter()
                .map(|v| inject_vector_placeholder(v, vector))
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, inject_vector_placeholder(v, vector)))
                .collect(),
        ),
        other => other,
    }
}

pub fn recall_defaults(queries: &Value) -> (u64, u64) {
    let defs = queries
        .get("recall_defaults")
        .expect("queries.recall_defaults");
    let num = defs["num"].as_u64().expect("recall_defaults.num");
    let top_k = defs["top_k"].as_u64().expect("recall_defaults.top_k");
    (num, top_k)
}

pub fn cold_query_protocol(queries: &Value) -> Value {
    queries
        .get("cold_query_protocol")
        .expect("queries.cold_query_protocol")
        .clone()
}

pub fn filter_query_specs(queries: &Value) -> &[Value] {
    queries["filter_queries"]
        .as_array()
        .expect("queries.filter_queries")
}

pub fn hybrid_query_specs(queries: &Value) -> &[Value] {
    queries["hybrid_queries"]
        .as_array()
        .expect("queries.hybrid_queries")
}

/// G2 gate expects the full precomputed filter + hybrid sets (not only `[0]` smoke).
pub fn assert_workload_filter_hybrid_counts(queries: &Value) {
    assert_eq!(
        filter_query_specs(queries).len(),
        EXPECTED_FILTER_QUERY_COUNT,
        "filter_queries count"
    );
    assert_eq!(
        hybrid_query_specs(queries).len(),
        EXPECTED_HYBRID_QUERY_COUNT,
        "hybrid_queries count"
    );
}

/// Assert every precomputed vector in `queries.json` matches `bench_sin_v1` for its doc index.
pub fn assert_queries_vectors_match_bench_sin(queries: &Value, dim: usize, epsilon: f64) {
    let check_spec = |spec: &Value, context: &str| {
        let doc_index = spec["doc_index"]
            .as_u64()
            .or_else(|| spec.get("reference_doc_index").and_then(|v| v.as_u64()))
            .expect(&format!("{context}: doc_index"));
        let doc_index = doc_index as usize;
        let expected = bench_sin_embedding(doc_index, dim);
        let actual: Vec<f64> = spec["vector"]
            .as_array()
            .expect(&format!("{context}: vector"))
            .iter()
            .map(|v| v.as_f64().expect(&format!("{context}: vector element")))
            .collect();
        assert_eq!(
            actual.len(),
            expected.len(),
            "{context}: vector dim mismatch"
        );
        for (d, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
            assert!(
                (a - e).abs() <= epsilon,
                "{context}: doc_index={doc_index} dim={d} expected={e} actual={a}"
            );
        }
    };

    for (i, spec) in queries["vector_queries"]
        .as_array()
        .expect("vector_queries")
        .iter()
        .enumerate()
    {
        check_spec(spec, &format!("vector_queries[{i}]"));
    }
    for (i, spec) in queries["filter_queries"]
        .as_array()
        .expect("filter_queries")
        .iter()
        .enumerate()
    {
        check_spec(spec, &format!("filter_queries[{i}]"));
    }
    for (i, spec) in queries["hybrid_queries"]
        .as_array()
        .expect("hybrid_queries")
        .iter()
        .enumerate()
    {
        check_spec(spec, &format!("hybrid_queries[{i}]"));
    }
}

/// Manifest `num_docs` / `dim` must match `queries.json` header fields.
pub fn assert_manifest_queries_consistent(manifest: &Value, queries: &Value) {
    for key in ["seed", "num_docs", "dim", "embedding_fn"] {
        assert_eq!(
            manifest.get(key),
            queries.get(key),
            "manifest vs queries mismatch on {key}"
        );
    }
    assert_eq!(
        manifest["workload"].as_str(),
        Some("synthetic-128"),
        "manifest.workload"
    );
}