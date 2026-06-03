//! Vector ANN index: SPFresh-inspired centroid / cluster layout on S3.
//!
//! Layout:
//! - `openpuffer/{ns}/index/centroids.bin` — centroid table + metadata
//! - `openpuffer/{ns}/index/clusters-{centroid_id:08}.bin` — doc ids + vectors per cluster

use crate::meta::DistanceMetric;
use crate::models::Document;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// How many nearest centroids to probe at query time (v1 default).
pub const DEFAULT_PROBE_CLUSTERS: u32 = 8;

/// Max centroids for v1 k-means.
const MAX_CENTROIDS: usize = 256;

/// k-means iterations when building.
const KMEANS_ITERS: usize = 10;

/// One document vector stored in a cluster segment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterMember {
    pub doc_id: String,
    pub vector: Vec<f64>,
}

/// Centroid table written to `centroids.bin`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CentroidIndex {
    pub segment_id: u64,
    pub vector_field: String,
    pub dimensions: u32,
    pub num_centroids: u32,
    pub probe_clusters: u32,
    pub distance_metric: DistanceMetric,
    pub centroids: Vec<Vec<f64>>,
}

impl Default for CentroidIndex {
    fn default() -> Self {
        Self {
            segment_id: 0,
            vector_field: String::new(),
            dimensions: 0,
            num_centroids: 0,
            probe_clusters: DEFAULT_PROBE_CLUSTERS,
            distance_metric: DistanceMetric::default(),
            centroids: Vec::new(),
        }
    }
}

impl CentroidIndex {
    pub fn key(namespace: &str) -> String {
        format!(
            "{}{namespace}/index/centroids.bin",
            crate::models::ROOT_PREFIX
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context("encode CentroidIndex")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context("decode CentroidIndex")
    }

    /// Top-M centroid ids by score (higher is better) for a query vector.
    pub fn nearest_centroids(&self, query: &[f64], m: usize) -> Vec<u32> {
        if self.centroids.is_empty() {
            return Vec::new();
        }
        let m = m.min(self.centroids.len());
        let mut ranked: Vec<(u32, f64)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| {
                (
                    i as u32,
                    score_vector(query, c, self.distance_metric),
                )
            })
            .collect();
        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        ranked.into_iter().take(m).map(|(id, _)| id).collect()
    }
}

/// One cluster segment: all doc vectors assigned to a centroid.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterSegment {
    pub segment_id: u64,
    pub centroid_id: u32,
    pub members: Vec<ClusterMember>,
}

impl ClusterSegment {
    pub fn key(namespace: &str, centroid_id: u32) -> String {
        format!(
            "{}{namespace}/index/clusters-{centroid_id:08}.bin",
            crate::models::ROOT_PREFIX
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context("encode ClusterSegment")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context("decode ClusterSegment")
    }

    /// Score all members against query; returns (doc_id, score) sorted desc.
    pub fn score_members(
        &self,
        query: &[f64],
        metric: DistanceMetric,
        top_k: usize,
    ) -> Vec<(String, f64)> {
        let mut scored: Vec<(String, f64)> = self
            .members
            .iter()
            .map(|m| {
                (
                    m.doc_id.clone(),
                    score_vector(query, &m.vector, metric),
                )
            })
            .collect();
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scored.truncate(top_k);
        scored
    }
}

/// In-memory vector index for queries (centroids + loaded cluster segments).
#[derive(Debug, Clone, Default)]
pub struct VectorIndex {
    pub centroids: CentroidIndex,
    pub clusters: HashMap<u32, ClusterSegment>,
}

impl VectorIndex {
    /// Build centroid/cluster layout from documents and write-ready segments.
    pub fn build(
        segment_id: u64,
        field: &str,
        metric: DistanceMetric,
        docs: &[(String, Document)],
    ) -> Result<Option<Self>> {
        let mut pairs: Vec<(String, Vec<f64>)> = Vec::new();
        let mut dimensions = 0u32;

        for (id, doc) in docs {
            let Ok(vec) = extract_vector(&doc.attributes, field) else {
                continue;
            };
            if vec.is_empty() {
                continue;
            }
            if dimensions == 0 {
                dimensions = vec.len() as u32;
            } else if vec.len() as u32 != dimensions {
                continue;
            }
            pairs.push((id.clone(), vec));
        }

        if pairs.is_empty() {
            return Ok(None);
        }

        let k = num_centroids(pairs.len());
        let centroid_vecs = kmeans_centroids(&pairs, k, dimensions as usize);
        let assignments = assign_to_centroids(&pairs, &centroid_vecs, metric);

        let mut clusters: HashMap<u32, ClusterSegment> = HashMap::new();
        for (doc_id, vec) in pairs {
            let cid = assignments.get(&doc_id).copied().unwrap_or(0);
            clusters
                .entry(cid)
                .or_insert_with(|| ClusterSegment {
                    segment_id,
                    centroid_id: cid,
                    members: Vec::new(),
                })
                .members
                .push(ClusterMember {
                    doc_id,
                    vector: vec,
                });
        }

        let centroids = CentroidIndex {
            segment_id,
            vector_field: field.to_string(),
            dimensions,
            num_centroids: centroid_vecs.len() as u32,
            probe_clusters: DEFAULT_PROBE_CLUSTERS.min(centroid_vecs.len() as u32).max(1),
            distance_metric: metric,
            centroids: centroid_vecs,
        };

        Ok(Some(VectorIndex {
            centroids,
            clusters,
        }))
    }

    /// Doc ids reachable by probing nearest centroids (candidate generation, no scoring).
    pub fn candidate_doc_ids(&self, query: &[f64]) -> HashSet<String> {
        if query.len() != self.centroids.dimensions as usize {
            return HashSet::new();
        }
        let m = if self.centroids.num_centroids <= 32 {
            self.centroids.num_centroids as usize
        } else {
            self.centroids
                .probe_clusters
                .min(self.centroids.num_centroids)
                .max(1) as usize
        };
        let probe = self.centroids.nearest_centroids(query, m);
        let mut ids = HashSet::new();
        for cid in probe {
            if let Some(cluster) = self.clusters.get(&cid) {
                for m in &cluster.members {
                    ids.insert(m.doc_id.clone());
                }
            }
        }
        ids
    }

    /// ANN query: probe nearest centroids, score cluster members, return top-k.
    pub fn query_ann(&self, query: &[f64], top_k: usize) -> Vec<(String, f64)> {
        if query.len() != self.centroids.dimensions as usize {
            return Vec::new();
        }
        // Small indexes: probe every cluster so ANN matches exhaustive (tests + tiny namespaces).
        let m = if self.centroids.num_centroids <= 32 {
            self.centroids.num_centroids as usize
        } else {
            self.centroids
                .probe_clusters
                .min(self.centroids.num_centroids)
                .max(1) as usize
        };
        let probe = self.centroids.nearest_centroids(query, m);
        let metric = self.centroids.distance_metric;

        let mut scores: HashMap<String, f64> = HashMap::new();
        for cid in probe {
            let Some(cluster) = self.clusters.get(&cid) else {
                continue;
            };
            for (id, score) in cluster.score_members(query, metric, top_k.saturating_mul(4)) {
                scores
                    .entry(id)
                    .and_modify(|s| {
                        if score > *s {
                            *s = score;
                        }
                    })
                    .or_insert(score);
            }
        }

        let mut ranked: Vec<(String, f64)> = scores.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        ranked.truncate(top_k);
        ranked
    }
}

fn num_centroids(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let sqrt_k = (n as f64).sqrt().ceil() as usize;
    sqrt_k.clamp(1, n).min(MAX_CENTROIDS)
}

/// Simple k-means; falls back to random doc vectors as centroids when n is small.
fn kmeans_centroids(pairs: &[(String, Vec<f64>)], k: usize, dim: usize) -> Vec<Vec<f64>> {
    let n = pairs.len();
    if n == 0 {
        return Vec::new();
    }
    if k >= n {
        return pairs.iter().map(|(_, v)| v.clone()).collect();
    }

    // Deterministic seed centroids: first k document vectors (v1; no external RNG).
    let mut centroids: Vec<Vec<f64>> = pairs
        .iter()
        .take(k)
        .map(|(_, v)| v.clone())
        .collect();

    for _ in 0..KMEANS_ITERS {
        let mut sums: Vec<Vec<f64>> = vec![vec![0.0; dim]; k];
        let mut counts = vec![0usize; k];

        for (_, v) in pairs {
            let best = nearest_centroid_id(v, &centroids, DistanceMetric::CosineDistance);
            for d in 0..dim {
                sums[best][d] += v[d];
            }
            counts[best] += 1;
        }

        for i in 0..k {
            if counts[i] > 0 {
                let inv = 1.0 / counts[i] as f64;
                for d in 0..dim {
                    centroids[i][d] = sums[i][d] * inv;
                }
            }
        }
    }
    centroids
}

fn assign_to_centroids(
    pairs: &[(String, Vec<f64>)],
    centroids: &[Vec<f64>],
    metric: DistanceMetric,
) -> HashMap<String, u32> {
    let mut out = HashMap::new();
    for (id, v) in pairs {
        let cid = nearest_centroid_id(v, centroids, metric);
        out.insert(id.clone(), cid as u32);
    }
    out
}

fn nearest_centroid_id(vec: &[f64], centroids: &[Vec<f64>], metric: DistanceMetric) -> usize {
    let mut best = 0usize;
    let mut best_score = f64::NEG_INFINITY;
    for (i, c) in centroids.iter().enumerate() {
        let s = score_vector(vec, c, metric);
        if s > best_score {
            best_score = s;
            best = i;
        }
    }
    best
}

/// Score query vs candidate (higher is better).
pub fn score_vector(query: &[f64], candidate: &[f64], metric: DistanceMetric) -> f64 {
    match metric {
        DistanceMetric::CosineDistance => cosine_similarity(query, candidate),
        DistanceMetric::EuclideanSquared => {
            let d2 = euclidean_squared(query, candidate);
            if d2.is_finite() {
                -d2
            } else {
                f64::NEG_INFINITY
            }
        }
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

fn euclidean_squared(a: &[f64], b: &[f64]) -> f64 {
    if a.len() != b.len() {
        return f64::INFINITY;
    }
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = x - y;
            d * d
        })
        .sum()
}

/// Extract vector from document attributes (shared with search).
pub fn extract_vector(attrs: &HashMap<String, Value>, field: &str) -> Result<Vec<f64>> {
    let v = attrs
        .get(field)
        .ok_or_else(|| anyhow::anyhow!("missing vector field {field}"))?;
    value_to_f64_vec(v)
}

pub fn value_to_f64_vec(v: &Value) -> Result<Vec<f64>> {
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("expected vector array"))?;
    arr.iter()
        .map(|x| {
            x.as_f64()
                .or_else(|| x.as_i64().map(|i| i as f64))
                .ok_or_else(|| anyhow::anyhow!("vector element must be number"))
        })
        .collect()
}

/// Vector fields from schema hints (`[]f32`, `vector`, etc.).
pub fn vector_fields_from_schema(schema: &Value) -> Vec<String> {
    let Some(obj) = schema.as_object() else {
        return Vec::new();
    };
    let mut fields = Vec::new();
    for (name, spec) in obj {
        if field_is_vector(spec) {
            fields.push(name.clone());
        }
    }
    fields
}

fn field_is_vector(spec: &Value) -> bool {
    match spec {
        Value::String(s) => {
            let t = s.to_lowercase();
            t.contains("f32") || t.contains("vector") || t.contains("[]f")
        }
        Value::Object(m) => {
            if let Some(Value::String(t)) = m.get("type") {
                let t = t.to_lowercase();
                return t.contains("f32") || t.contains("vector") || t.contains("[]f");
            }
            false
        }
        _ => false,
    }
}

/// Pick primary vector field (first schema vector field, or first f64 array attr seen).
pub fn primary_vector_field(schema: &Value, sample: Option<&Document>) -> Option<String> {
    let fields = vector_fields_from_schema(schema);
    if let Some(f) = fields.into_iter().next() {
        return Some(f);
    }
    if let Some(doc) = sample {
        for (name, v) in &doc.attributes {
            if value_to_f64_vec(v).is_ok() {
                return Some(name.clone());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn ann_100_docs_4dim_returns_nearest_neighbor() {
        let mut docs = Vec::new();
        for i in 0..100 {
            let x = (i as f64) * 0.01;
            docs.push(vec_doc(
                &format!("doc-{i}"),
                vec![x, 1.0 - x, 0.5, 0.0],
            ));
        }
        // Unique nearest neighbor to query [1,0,0.5,0]
        docs.push(vec_doc("target", vec![1.0, 0.0, 0.5, 0.0]));

        let index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
        )
        .unwrap()
        .expect("index built");

        let query = vec![1.0, 0.0, 0.5, 0.0];
        let hits = index.query_ann(&query, 1);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "target");
        assert!(hits[0].1 > 0.99);
    }

    #[test]
    fn centroid_index_roundtrip_bincode() {
        let idx = CentroidIndex {
            segment_id: 3,
            vector_field: "emb".into(),
            dimensions: 2,
            num_centroids: 2,
            probe_clusters: 2,
            distance_metric: DistanceMetric::CosineDistance,
            centroids: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
        };
        let bytes = idx.encode().unwrap();
        let back = CentroidIndex::decode(&bytes).unwrap();
        assert_eq!(back.segment_id, 3);
        assert_eq!(back.centroids.len(), 2);
    }

    #[test]
    fn cluster_segment_roundtrip() {
        let seg = ClusterSegment {
            segment_id: 1,
            centroid_id: 0,
            members: vec![ClusterMember {
                doc_id: "a".into(),
                vector: vec![1.0, 0.0],
            }],
        };
        let bytes = seg.encode().unwrap();
        let back = ClusterSegment::decode(&bytes).unwrap();
        assert_eq!(back.members[0].doc_id, "a");
    }
}