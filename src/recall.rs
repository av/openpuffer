//! Namespace recall evaluation (turbopuffer `POST /v1/namespaces/{name}/recall`).

use crate::filter::parse_filter;
use crate::index::vector::{
    brute_force_top_k, extract_vector, primary_vector_field, VectorIndex,
};
use crate::models::Document;
use crate::search::{matching_doc_ids_for_filter, QueryConsistency, QueryContext};
use crate::storage::LoadedNamespace;
use anyhow::{anyhow, Result};
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Mean recall and result counts across sampled queries (turbopuffer recall response).
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct RecallMetrics {
    pub avg_recall: f64,
    pub avg_ann_count: f64,
    pub avg_exhaustive_count: f64,
}

/// Sample `num` distinct corpus indices using a deterministic LCG (`seed`).
fn sample_corpus_indices(corpus_len: usize, num: usize, seed: u64) -> Vec<usize> {
    if corpus_len == 0 || num == 0 {
        return Vec::new();
    }
    let want = num.min(corpus_len);
    let mut picked = HashSet::with_capacity(want);
    let mut out = Vec::with_capacity(want);
    let mut state = seed;
    while out.len() < want {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let idx = (state as usize) % corpus_len;
        if picked.insert(idx) {
            out.push(idx);
        }
    }
    out
}

/// Evaluate recall@*top_k* on `num` pseudo-random queries from `corpus`.
///
/// ANN results use `query_ann` (probe-only) or `rerank_top_k` when `use_rerank` is true.
/// Exhaustive ground truth is brute-force over the same `corpus`.
pub fn measure_recall(
    index: &VectorIndex,
    corpus: &[(String, Vec<f64>)],
    num: usize,
    top_k: usize,
    use_rerank: bool,
    view_vector: impl Fn(&str) -> Option<Vec<f64>>,
    seed: u64,
) -> RecallMetrics {
    if corpus.is_empty() || num == 0 || top_k == 0 {
        return RecallMetrics {
            avg_recall: 0.0,
            avg_ann_count: 0.0,
            avg_exhaustive_count: 0.0,
        };
    }
    let metric = index.l0.distance_metric;
    let samples = sample_corpus_indices(corpus.len(), num, seed);
    let n = samples.len() as f64;
    let mut recall_sum = 0.0f64;
    let mut ann_count_sum = 0.0f64;
    let mut exhaustive_count_sum = 0.0f64;

    for idx in samples {
        let query = corpus[idx].1.clone();
        let brute = brute_force_top_k(corpus, &query, metric, top_k);
        let ann_results: Vec<String> = if use_rerank {
            index
                .rerank_top_k(&query, top_k, &view_vector)
                .into_iter()
                .map(|(id, _)| id)
                .collect()
        } else {
            index
                .query_ann(&query, top_k)
                .into_iter()
                .map(|(id, _)| id)
                .collect()
        };
        let ann_set: HashSet<_> = ann_results.iter().cloned().collect();
        let hits = brute.iter().filter(|id| ann_set.contains(*id)).count();
        recall_sum += hits as f64 / top_k as f64;
        ann_count_sum += ann_results.len() as f64;
        exhaustive_count_sum += brute.len() as f64;
    }

    RecallMetrics {
        avg_recall: recall_sum / n,
        avg_ann_count: ann_count_sum / n,
        avg_exhaustive_count: exhaustive_count_sum / n,
    }
}

fn recall_seed(namespace: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in namespace.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn build_corpus(
    docs: &HashMap<String, Document>,
    field: &str,
    allowed: Option<&HashSet<String>>,
) -> Vec<(String, Vec<f64>)> {
    let mut corpus = Vec::new();
    for (id, doc) in docs {
        if allowed.is_some_and(|a| !a.contains(id)) {
            continue;
        }
        if let Ok(v) = extract_vector(&doc.attributes, field) {
            corpus.push((id.clone(), v));
        }
    }
    corpus.sort_by(|a, b| a.0.cmp(&b.0));
    corpus
}

/// Run recall on a loaded namespace (strong consistency, indexed ANN).
pub fn measure_recall_for_loaded(
    loaded: &LoadedNamespace,
    field: &str,
    num: usize,
    top_k: usize,
    use_rerank: bool,
    filters: Option<&Value>,
    namespace: &str,
) -> Result<RecallMetrics> {
    let index = loaded
        .vectors
        .get(field)
        .ok_or_else(|| anyhow!("no vector index for field {field}"))?;

    let allowed = if let Some(f) = filters.filter(|v| !v.is_null()) {
        let expr = parse_filter(f)?;
        let ctx = QueryContext {
            cold_s3_keys_fetched: None,
            ann_probed_clusters: None,
            docs: &loaded.docs,
            meta: &loaded.meta,
            fts: loaded.fts.as_ref(),
            vectors: &loaded.vectors,
            filter_index: loaded.filter_index.as_ref(),
            tail_doc_ids: &loaded.tail_doc_ids,
            consistency: QueryConsistency::Strong,
            storage_roundtrips: loaded.storage_roundtrips,
            ann_rerank: Some(use_rerank),
        };
        Some(matching_doc_ids_for_filter(&ctx, &expr)?)
    } else {
        None
    };

    let corpus = build_corpus(&loaded.docs, field, allowed.as_ref());
    if corpus.is_empty() {
        return Err(anyhow!("no documents with vector field {field}"));
    }

    let view_vector = |id: &str| -> Option<Vec<f64>> {
        loaded
            .docs
            .get(id)
            .and_then(|d| extract_vector(&d.attributes, field).ok())
    };

    Ok(measure_recall(
        index,
        &corpus,
        num,
        top_k,
        use_rerank,
        view_vector,
        recall_seed(namespace),
    ))
}

/// Resolve the vector field to evaluate (schema hint or first indexed field).
pub fn recall_vector_field(loaded: &LoadedNamespace) -> Result<String> {
    let sample = loaded.docs.values().next();
    primary_vector_field(&loaded.meta.schema, sample)
        .or_else(|| loaded.vectors.keys().next().cloned())
        .or_else(|| loaded.cold_vector_l0.keys().next().cloned())
        .or_else(|| {
            crate::meta::effective_vector_fields(&loaded.meta)
                .first()
                .map(|f| f.name.clone())
        })
        .ok_or_else(|| anyhow!("namespace has no vector field"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AnnBuildConfig;
    use crate::index::vector::VectorIndex;
    use crate::meta::DistanceMetric;
    use crate::models::Document;
    use serde_json::json;

    fn vec_doc(id: &str, embedding: Vec<f64>) -> (String, Document) {
        (
            id.to_string(),
            Document {
                id: id.to_string(),
                attributes: [("embedding".into(), json!(embedding))].into(),
            },
        )
    }

    #[test]
    fn measure_recall_returns_expected_counts_on_tiny_fixture() {
        let docs: Vec<_> = (0..8)
            .map(|i| {
                let mut v = vec![0.0; 4];
                v[i % 4] = 1.0;
                vec_doc(&format!("d{i}"), v)
            })
            .collect();
        let index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
            &json!({"embedding": "[4]f32"}),
            AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("index built");
        let corpus: Vec<_> = docs
            .iter()
            .map(|(id, d)| {
                (
                    id.clone(),
                    extract_vector(&d.attributes, "embedding").unwrap(),
                )
            })
            .collect();
        let view = |id: &str| corpus.iter().find(|(i, _)| i == id).map(|(_, v)| v.clone());
        let m = measure_recall(&index, &corpus, 5, 4, false, view, 42);
        assert!(
            m.avg_recall >= 0.5,
            "avg_recall {} on tiny fixture",
            m.avg_recall
        );
        assert_eq!(m.avg_ann_count, 4.0);
        assert_eq!(m.avg_exhaustive_count, 4.0);
    }
}