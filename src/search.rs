use crate::index::fts::{bm25_doc_score, extract_index_text, FtsSegment};
use crate::index::vector::{extract_vector, score_vector, value_to_f64_vec, VectorIndex};

use crate::meta::NamespaceMeta;
use crate::models::{Document, QueryRequest, QueryResponse, QueryRow};
use anyhow::{anyhow, bail, Result};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct QueryContext<'a> {
    pub docs: &'a HashMap<String, Document>,
    pub meta: &'a NamespaceMeta,
    pub fts: Option<&'a FtsSegment>,
    pub vector: Option<&'a VectorIndex>,
    pub tail_doc_ids: &'a HashSet<String>,
}

#[derive(Debug, Clone)]
enum Ranker {
    Vector {
        field: String,
        query: Vec<f64>,
    },
    Bm25 {
        field: String,
        query: String,
    },
    Sum(Vec<Ranker>),
    Product(Vec<Ranker>),
}

pub fn execute_query(ctx: &QueryContext<'_>, req: &QueryRequest) -> Result<QueryResponse> {
    if let Some(filters) = &req.filters {
        if !filters.is_null() {
            bail!("filters are not supported in v1");
        }
    }

    let top_k = req.top_k.unwrap_or(10) as usize;
    let ranker = parse_rank_by(&req.rank_by)?;
    let mut scored: Vec<(String, f64)> = Vec::new();

    match &ranker {
        Ranker::Bm25 { field, query } if ctx.fts.is_some() => {
            scored = execute_bm25_indexed(ctx, field, query, top_k)?;
        }
        Ranker::Vector { field, query } if ctx.vector.is_some() => {
            scored = execute_vector_indexed(ctx, field, query, top_k)?;
        }
        _ => {
            for (id, doc) in ctx.docs {
                let score = score_doc(doc, &ranker, ctx)?;
                if score.is_finite() {
                    scored.push((id.clone(), score));
                }
            }
        }
    }

    scored.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    scored.truncate(top_k);

    let include_attrs = match &req.include_attributes {
        None => true,
        Some(Value::Bool(false)) => false,
        Some(Value::Bool(true)) => true,
        Some(_) => true,
    };
    let rows = scored
        .into_iter()
        .map(|(id, score)| {
            let doc = ctx.docs.get(&id);
            let attributes = if include_attrs {
                doc.map(|d| d.attributes.clone())
            } else {
                None
            };
            QueryRow {
                id,
                attributes,
                dist: Some(score),
            }
        })
        .collect();

    Ok(QueryResponse { rows })
}

/// BM25 via FTS posting lists for indexed docs + exhaustive scan on unindexed WAL tail only.
fn execute_bm25_indexed(
    ctx: &QueryContext<'_>,
    field: &str,
    query: &str,
    top_k: usize,
) -> Result<Vec<(String, f64)>> {
    let fts = ctx.fts.expect("caller ensures fts is present");
    let fts_field = if fts.field.is_empty() { field } else { &fts.field };

    let mut scores: HashMap<String, f64> = HashMap::new();
    let indexed_hits = fts.query_bm25(query, top_k.saturating_mul(4).max(32));
    for (id, score) in indexed_hits {
        if ctx.tail_doc_ids.contains(&id) {
            continue;
        }
        if !ctx.docs.contains_key(&id) {
            continue;
        }
        scores.insert(id, score);
    }

    let avgdl = fts.avg_doc_len();
    let num_docs = fts.num_docs.max(1);
    for id in ctx.tail_doc_ids {
        let Some(doc) = ctx.docs.get(id) else {
            continue;
        };
        let text = extract_index_text(doc, fts_field);
        let score = bm25_doc_score(&text, query, avgdl, num_docs);
        if score > 0.0 && score.is_finite() {
            scores.insert(id.clone(), score);
        }
    }

    let mut ranked: Vec<(String, f64)> = scores.into_iter().collect();
    ranked.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    ranked.truncate(top_k);
    Ok(ranked)
}

/// ANN via centroid/cluster probe for indexed docs + exhaustive scan on unindexed WAL tail only.
fn execute_vector_indexed(
    ctx: &QueryContext<'_>,
    field: &str,
    query: &[f64],
    top_k: usize,
) -> Result<Vec<(String, f64)>> {
    let vindex = ctx.vector.expect("caller ensures vector index is present");
    let vfield = if vindex.centroids.vector_field.is_empty() {
        field
    } else {
        &vindex.centroids.vector_field
    };
    let metric = ctx.meta.distance_metric;

    let mut scores: HashMap<String, f64> = HashMap::new();
    if query.len() == vindex.centroids.dimensions as usize {
        for (id, score) in vindex.query_ann(query, top_k.saturating_mul(4).max(32)) {
            if ctx.tail_doc_ids.contains(&id) {
                continue;
            }
            if !ctx.docs.contains_key(&id) {
                continue;
            }
            scores.insert(id, score);
        }
    }

    for id in ctx.tail_doc_ids {
        let Some(doc) = ctx.docs.get(id) else {
            continue;
        };
        let Ok(doc_vec) = extract_vector(&doc.attributes, vfield) else {
            continue;
        };
        if doc_vec.len() != query.len() {
            continue;
        }
        let score = score_vector(query, &doc_vec, metric);
        if score.is_finite() {
            scores.insert(id.clone(), score);
        }
    }

    let mut ranked: Vec<(String, f64)> = scores.into_iter().collect();
    ranked.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    ranked.truncate(top_k);
    Ok(ranked)
}

fn parse_rank_by(v: &Value) -> Result<Ranker> {
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow!("rank_by must be an array"))?;
    if arr.is_empty() {
        bail!("rank_by array is empty");
    }
    let head = arr[0].as_str().ok_or_else(|| anyhow!("rank_by[0] must be string"))?;
    match head {
        "vector" | "Vector" => {
            if arr.len() < 4 {
                bail!("vector rank_by needs [vector, ANN, field, query]");
            }
            let field = arr[2].as_str().ok_or_else(|| anyhow!("field must be string"))?;
            let query = value_to_f64_vec(&arr[3])?;
            Ok(Ranker::Vector {
                field: field.to_string(),
                query,
            })
        }
        "BM25" | "bm25" => {
            if arr.len() < 3 {
                bail!("BM25 rank_by needs [BM25, field, query]");
            }
            let field = arr[1].as_str().ok_or_else(|| anyhow!("field must be string"))?;
            let query = arr[2]
                .as_str()
                .ok_or_else(|| anyhow!("query must be string"))?;
            Ok(Ranker::Bm25 {
                field: field.to_string(),
                query: query.to_string(),
            })
        }
        "Sum" | "sum" => {
            let subs = arr[1..]
                .iter()
                .map(parse_rank_by)
                .collect::<Result<Vec<_>>>()?;
            Ok(Ranker::Sum(subs))
        }
        "Product" | "product" => {
            let subs = arr[1..]
                .iter()
                .map(parse_rank_by)
                .collect::<Result<Vec<_>>>()?;
            Ok(Ranker::Product(subs))
        }
        other => bail!("unknown rank_by operator: {other}"),
    }
}

fn score_doc(doc: &Document, ranker: &Ranker, ctx: &QueryContext<'_>) -> Result<f64> {
    match ranker {
        Ranker::Vector { field, query } => {
            let doc_vec = extract_vector(&doc.attributes, field)?;
            Ok(score_vector(query, &doc_vec, ctx.meta.distance_metric))
        }
        Ranker::Bm25 { field, query } => {
            let text = extract_index_text(doc, field);
            if let Some(fts) = ctx.fts {
                let score = bm25_doc_score(&text, query, fts.avg_doc_len(), fts.num_docs.max(1));
                Ok(score)
            } else {
                Ok(bm25_score_legacy(&text, query))
            }
        }
        Ranker::Sum(subs) => {
            let mut total = 0.0;
            for sub in subs {
                total += normalize_score(score_doc(doc, sub, ctx)?);
            }
            Ok(total)
        }
        Ranker::Product(subs) => {
            let mut prod = 1.0;
            for sub in subs {
                prod *= normalize_score(score_doc(doc, sub, ctx)?);
            }
            Ok(prod)
        }
    }
}

fn normalize_score(s: f64) -> f64 {
    if s.is_nan() {
        0.0
    } else {
        s.clamp(0.0, 1.0)
    }
}

/// Cosine similarity (higher is better). Re-exported for tests and legacy callers.
pub fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    crate::index::vector::cosine_similarity(a, b)
}

/// Legacy per-doc BM25 when no FTS index is available (full scan fallback).
pub fn bm25_score_legacy(document: &str, query: &str) -> f64 {
    bm25_doc_score(document, query, document.split_whitespace().count().max(1) as f64, 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::fts::FtsSegment;
    use crate::meta::NamespaceMeta;
    use crate::models::{Document, QueryRequest};
    use serde_json::json;

    #[test]
    fn cosine_identical() {
        let v = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&v, &v) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn bm25_indexed_query_returns_top_doc() {
        let mut map: HashMap<String, Document> = HashMap::new();
        map.insert(
            "a".into(),
            Document {
                id: "a".into(),
                attributes: [("text".into(), json!("rust fast programming"))].into(),
            },
        );
        map.insert(
            "b".into(),
            Document {
                id: "b".into(),
                attributes: [("text".into(), json!("python slow scripting"))].into(),
            },
        );
        let pairs: Vec<(String, Document)> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let seg = FtsSegment::build(1, "text", &pairs);
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 1,
            fts_segment_id: 1,
            ..Default::default()
        };
        let tail = HashSet::new();
        let ctx = QueryContext {
            docs: &map,
            meta: &meta,
            fts: Some(&seg),
            vector: None,
            tail_doc_ids: &tail,
        };
        let req = QueryRequest {
            rank_by: json!(["BM25", "text", "rust programming"]),
            top_k: Some(1),
            filters: None,
            include_attributes: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        assert_eq!(resp.rows[0].id, "a");
        assert!(resp.rows[0].dist.unwrap() > 0.0);
    }

    #[test]
    fn tail_doc_uses_exhaustive_not_stale_index() {
        let mut map = HashMap::new();
        map.insert(
            "a".into(),
            Document {
                id: "a".into(),
                attributes: [("text".into(), json!("old content"))].into(),
            },
        );
        let indexed = vec![(
            "a".into(),
            Document {
                id: "a".into(),
                attributes: [("text".into(), json!("rust fast programming"))].into(),
            },
        )];
        let seg = FtsSegment::build(1, "text", &indexed);
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 2,
            fts_segment_id: 1,
            ..Default::default()
        };
        let mut tail = HashSet::new();
        tail.insert("a".into());
        let ctx = QueryContext {
            docs: &map,
            meta: &meta,
            fts: Some(&seg),
            vector: None,
            tail_doc_ids: &tail,
        };
        let req = QueryRequest {
            rank_by: json!(["BM25", "text", "rust"]),
            top_k: Some(5),
            filters: None,
            include_attributes: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        // Current doc text has no "rust"; tail exhaustive should not return spurious high score from index.
        assert!(resp.rows.is_empty() || resp.rows[0].dist.unwrap_or(0.0) == 0.0);
    }

    #[test]
    fn include_attributes_false_omits_attrs() {
        let mut docs = HashMap::new();
        docs.insert(
            "a".into(),
            Document {
                id: "a".into(),
                attributes: [("text".into(), Value::String("hi".into()))]
                    .into_iter()
                    .collect(),
            },
        );
        let meta = NamespaceMeta::default();
        let tail = HashSet::new();
        let ctx = QueryContext {
            docs: &docs,
            meta: &meta,
            fts: None,
            vector: None,
            tail_doc_ids: &tail,
        };
        let req = QueryRequest {
            rank_by: Value::Array(vec![
                Value::String("BM25".into()),
                Value::String("text".into()),
                Value::String("hi".into()),
            ]),
            top_k: Some(1),
            filters: None,
            include_attributes: Some(Value::Bool(false)),
        };
        let resp = execute_query(&ctx, &req).unwrap();
        assert_eq!(resp.rows.len(), 1);
        assert!(resp.rows[0].attributes.is_none());
    }
}