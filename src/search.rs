use crate::models::{Document, QueryRequest, QueryResponse, QueryRow};
use anyhow::{anyhow, bail, Result};
use serde_json::Value;
use std::collections::HashMap;

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

pub fn execute_query(docs: &HashMap<String, Document>, req: &QueryRequest) -> Result<QueryResponse> {
    if let Some(filters) = &req.filters {
        if !filters.is_null() {
            bail!("filters are not supported in v1");
        }
    }

    let top_k = req.top_k.unwrap_or(10) as usize;
    let ranker = parse_rank_by(&req.rank_by)?;
    let mut scored: Vec<(String, f64)> = Vec::new();

    for (id, doc) in docs {
        let score = score_doc(doc, &ranker)?;
        if score.is_finite() {
            scored.push((id.clone(), score));
        }
    }

    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
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
            let doc = docs.get(&id);
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
            // ["vector", "ANN", field, query_vector]
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
            // ["BM25", field, query_string]
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

fn value_to_f64_vec(v: &Value) -> Result<Vec<f64>> {
    let arr = v.as_array().ok_or_else(|| anyhow!("expected vector array"))?;
    arr.iter()
        .map(|x| {
            x.as_f64()
                .or_else(|| x.as_i64().map(|i| i as f64))
                .ok_or_else(|| anyhow!("vector element must be number"))
        })
        .collect()
}

fn score_doc(doc: &Document, ranker: &Ranker) -> Result<f64> {
    match ranker {
        Ranker::Vector { field, query } => {
            let doc_vec = extract_vector(&doc.attributes, field)?;
            Ok(cosine_similarity(query, &doc_vec))
        }
        Ranker::Bm25 { field, query } => {
            let text = extract_text(&doc.attributes, field)?;
            Ok(bm25_score(&text, query))
        }
        Ranker::Sum(subs) => {
            let mut total = 0.0;
            for sub in subs {
                total += normalize_score(score_doc(doc, sub)?);
            }
            Ok(total)
        }
        Ranker::Product(subs) => {
            let mut prod = 1.0;
            for sub in subs {
                prod *= normalize_score(score_doc(doc, sub)?);
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

fn extract_vector(attrs: &HashMap<String, Value>, field: &str) -> Result<Vec<f64>> {
    let v = attrs
        .get(field)
        .ok_or_else(|| anyhow!("missing vector field {field}"))?;
    value_to_f64_vec(v)
}

fn extract_text(attrs: &HashMap<String, Value>, field: &str) -> Result<String> {
    let v = attrs
        .get(field)
        .ok_or_else(|| anyhow!("missing text field {field}"))?;
    match v {
        Value::String(s) => Ok(s.clone()),
        _ => Ok(v.to_string()),
    }
}

/// Cosine similarity (higher is better). Returns 0 for zero vectors.
pub fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0;
    let mut na = 0.0;
    let mut nb = 0.0;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// Simple BM25-style lexical score (higher is better).
pub fn bm25_score(document: &str, query: &str) -> f64 {
    let k1 = 1.2;
    let b = 0.75;
    let doc_tokens: Vec<&str> = document.split_whitespace().collect();
    let doc_len = doc_tokens.len().max(1) as f64;
    let avgdl = doc_len;
    let mut score = 0.0;
    for term in query.split_whitespace() {
        let tf = doc_tokens.iter().filter(|t| **t == term).count() as f64;
        if tf == 0.0 {
            continue;
        }
        let idf = ((1.0 + 1.0) / (1.0 + tf)).ln() + 1.0;
        let num = tf * (k1 + 1.0);
        let den = tf + k1 * (1.0 - b + b * doc_len / avgdl);
        score += idf * num / den;
    }
    score
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Document, QueryRequest};

    #[test]
    fn cosine_identical() {
        let v = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&v, &v) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn bm25_matches_keyword() {
        let s = bm25_score("hello world foo", "hello");
        assert!(s > 0.0);
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
        let resp = execute_query(&docs, &req).unwrap();
        assert_eq!(resp.rows.len(), 1);
        assert!(resp.rows[0].attributes.is_none());
    }
}