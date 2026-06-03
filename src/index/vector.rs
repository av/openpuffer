//! Vector ANN index: SPFresh-style two-level centroid / cluster layout on S3.
//!
//! Layout:
//! - `openpuffer/{ns}/index/centroids-l0.bin` — coarse centroid table + metadata
//! - `openpuffer/{ns}/index/centroids-l1-{coarse_id:08}.bin` — fine centroids per coarse cell
//! - `openpuffer/{ns}/index/clusters-{fine_id:08}.bin` — doc ids + vectors per fine centroid

use crate::meta::DistanceMetric;
use crate::models::Document;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// How many nearest coarse centroids to probe at query time.
pub const DEFAULT_PROBE_COARSE: u32 = 4;

/// Fine centroids to probe per selected coarse cell.
pub const DEFAULT_PROBE_FINE: u32 = 2;

/// Max coarse centroids (level 0).
pub const MAX_COARSE_CENTROIDS: usize = 16;

/// Max fine centroids per coarse cell.
const MAX_FINE_PER_COARSE: usize = 256;

/// k-means iterations when building.
const KMEANS_ITERS: usize = 10;

/// Re-run full hierarchy when doc count exceeds `num_fine_total * REBUILD_DOC_MULTIPLIER`.
pub const REBUILD_DOC_MULTIPLIER: usize = 4;

/// One document vector stored in a cluster segment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterMember {
    pub doc_id: String,
    pub vector: Vec<f64>,
}

/// Level-0 coarse centroid table (`centroids-l0.bin`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CentroidIndexL0 {
    pub segment_id: u64,
    pub vector_field: String,
    pub dimensions: u32,
    pub num_coarse: u32,
    pub num_fine_total: u32,
    pub probe_coarse: u32,
    pub probe_fine: u32,
    pub distance_metric: DistanceMetric,
    /// Fine centroid count per coarse bucket (defines global fine id offsets).
    pub fine_counts: Vec<u32>,
    pub centroids: Vec<Vec<f64>>,
}

impl Default for CentroidIndexL0 {
    fn default() -> Self {
        Self {
            segment_id: 0,
            vector_field: String::new(),
            dimensions: 0,
            num_coarse: 0,
            num_fine_total: 0,
            probe_coarse: DEFAULT_PROBE_COARSE,
            probe_fine: DEFAULT_PROBE_FINE,
            distance_metric: DistanceMetric::default(),
            fine_counts: Vec::new(),
            centroids: Vec::new(),
        }
    }
}

impl CentroidIndexL0 {
    pub fn key(namespace: &str) -> String {
        format!(
            "{}{namespace}/index/centroids-l0.bin",
            crate::models::ROOT_PREFIX
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context("encode CentroidIndexL0")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context("decode CentroidIndexL0")
    }

    pub fn global_id_start(&self, coarse_id: u32) -> u32 {
        self.fine_counts
            .iter()
            .take(coarse_id as usize)
            .map(|&c| c)
            .sum()
    }

    pub fn global_fine_id(&self, coarse_id: u32, local_fine: u32) -> u32 {
        self.global_id_start(coarse_id) + local_fine
    }

    /// Top-M coarse centroid ids by score (higher is better).
    pub fn nearest_coarse(&self, query: &[f64], m: usize) -> Vec<u32> {
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

    pub fn probe_coarse_count(&self) -> usize {
        if self.num_fine_total <= 32 {
            self.num_coarse as usize
        } else {
            self.probe_coarse
                .min(self.num_coarse)
                .max(1) as usize
        }
    }

    pub fn probe_fine_count(&self, l1: &CentroidIndexL1) -> usize {
        if self.num_fine_total <= 32 {
            l1.num_fine as usize
        } else {
            self.probe_fine.min(l1.num_fine).max(1) as usize
        }
    }
}

/// Level-1 fine centroids for one coarse cell (`centroids-l1-{coarse_id:08}.bin`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CentroidIndexL1 {
    pub segment_id: u64,
    pub coarse_id: u32,
    pub global_id_start: u32,
    pub num_fine: u32,
    pub centroids: Vec<Vec<f64>>,
}

impl CentroidIndexL1 {
    pub fn key(namespace: &str, coarse_id: u32) -> String {
        format!(
            "{}{namespace}/index/centroids-l1-{coarse_id:08}.bin",
            crate::models::ROOT_PREFIX
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context("encode CentroidIndexL1")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context("decode CentroidIndexL1")
    }

    pub fn nearest_fine(&self, query: &[f64], metric: DistanceMetric, m: usize) -> Vec<u32> {
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
                    score_vector(query, c, metric),
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

/// One cluster segment: all doc vectors assigned to a fine centroid.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterSegment {
    pub segment_id: u64,
    pub centroid_id: u32,
    pub members: Vec<ClusterMember>,
}

impl ClusterSegment {
    pub fn key(namespace: &str, fine_id: u32) -> String {
        format!(
            "{}{namespace}/index/clusters-{fine_id:08}.bin",
            crate::models::ROOT_PREFIX
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context("encode ClusterSegment")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context("decode ClusterSegment")
    }

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

/// In-memory vector index (L0 + L1 segments + cluster segments).
#[derive(Debug, Clone, Default)]
pub struct VectorIndex {
    pub l0: CentroidIndexL0,
    pub l1: HashMap<u32, CentroidIndexL1>,
    pub clusters: HashMap<u32, ClusterSegment>,
}

impl VectorIndex {
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

        let k_coarse = num_coarse(pairs.len());
        let coarse_vecs = kmeans_centroids(&pairs, k_coarse, dimensions as usize);
        let coarse_assign = assign_to_centroids(&pairs, &coarse_vecs, metric);

        let mut by_coarse: HashMap<u32, Vec<(String, Vec<f64>)>> = HashMap::new();
        for (doc_id, vec) in pairs {
            let coarse = coarse_assign.get(&doc_id).copied().unwrap_or(0);
            by_coarse.entry(coarse).or_default().push((doc_id, vec));
        }

        let mut l1_map: HashMap<u32, CentroidIndexL1> = HashMap::new();
        let mut clusters: HashMap<u32, ClusterSegment> = HashMap::new();
        let mut fine_counts: Vec<u32> = vec![0; k_coarse];
        let mut global_start = 0u32;

        for coarse_id in 0..k_coarse as u32 {
            let cell_docs = by_coarse.remove(&coarse_id).unwrap_or_default();
            let k_fine = num_fine(cell_docs.len());
            fine_counts[coarse_id as usize] = k_fine as u32;

            let fine_vecs = if cell_docs.is_empty() {
                Vec::new()
            } else {
                kmeans_centroids(&cell_docs, k_fine, dimensions as usize)
            };

            let fine_assign = assign_to_centroids(&cell_docs, &fine_vecs, metric);

            l1_map.insert(
                coarse_id,
                CentroidIndexL1 {
                    segment_id,
                    coarse_id,
                    global_id_start: global_start,
                    num_fine: fine_vecs.len() as u32,
                    centroids: fine_vecs,
                },
            );

            for (doc_id, vec) in cell_docs {
                let local_fine = fine_assign.get(&doc_id).copied().unwrap_or(0);
                let fine_id = global_start + local_fine;
                clusters
                    .entry(fine_id)
                    .or_insert_with(|| ClusterSegment {
                        segment_id,
                        centroid_id: fine_id,
                        members: Vec::new(),
                    })
                    .members
                    .push(ClusterMember {
                        doc_id,
                        vector: vec,
                    });
            }

            global_start += k_fine as u32;
        }

        let num_fine_total = global_start;
        let l0 = CentroidIndexL0 {
            segment_id,
            vector_field: field.to_string(),
            dimensions,
            num_coarse: k_coarse as u32,
            num_fine_total,
            probe_coarse: DEFAULT_PROBE_COARSE.min(k_coarse as u32).max(1),
            probe_fine: DEFAULT_PROBE_FINE,
            distance_metric: metric,
            fine_counts,
            centroids: coarse_vecs,
        };

        Ok(Some(VectorIndex {
            l0,
            l1: l1_map,
            clusters,
        }))
    }

    pub fn doc_count(&self) -> usize {
        self.clusters.values().map(|c| c.members.len()).sum()
    }

    pub fn needs_full_rebuild(&self) -> bool {
        let n = self.doc_count();
        let k = self.l0.num_fine_total as usize;
        if k == 0 || n == 0 {
            return true;
        }
        n > k.saturating_mul(REBUILD_DOC_MULTIPLIER)
    }

    pub fn apply_delta(
        &mut self,
        upserts: &[(String, Document)],
        deletes: &[String],
    ) -> Result<()> {
        let field = self.l0.vector_field.clone();
        let dim = self.l0.dimensions as usize;
        if dim == 0 || self.l0.centroids.is_empty() {
            return Ok(());
        }

        for id in deletes {
            self.remove_doc(id);
        }

        for (id, doc) in upserts {
            self.remove_doc(id);
            let Ok(vec) = extract_vector(&doc.attributes, &field) else {
                continue;
            };
            if vec.len() != dim {
                continue;
            }
            let coarse = self
                .l0
                .nearest_coarse(&vec, 1)
                .first()
                .copied()
                .unwrap_or(0);
            let l1 = self.l1.get(&coarse).ok_or_else(|| {
                anyhow::anyhow!("missing L1 segment for coarse {coarse}")
            })?;
            let local_fine = l1
                .nearest_fine(&vec, self.l0.distance_metric, 1)
                .first()
                .copied()
                .unwrap_or(0);
            let fine_id = self.l0.global_fine_id(coarse, local_fine);
            self.clusters
                .entry(fine_id)
                .or_insert_with(|| ClusterSegment {
                    segment_id: self.l0.segment_id,
                    centroid_id: fine_id,
                    members: Vec::new(),
                })
                .members
                .push(ClusterMember {
                    doc_id: id.clone(),
                    vector: vec,
                });
        }
        Ok(())
    }

    fn remove_doc(&mut self, doc_id: &str) {
        for cluster in self.clusters.values_mut() {
            cluster.members.retain(|m| m.doc_id != doc_id);
        }
    }

    /// Global fine centroid ids to probe for a query (two-level descent).
    pub fn probe_fine_centroids(&self, query: &[f64]) -> Vec<u32> {
        if query.len() != self.l0.dimensions as usize {
            return Vec::new();
        }
        let coarse_m = self.l0.probe_coarse_count();
        let coarse_ids = self.l0.nearest_coarse(query, coarse_m);
        let mut fine_ids = Vec::new();
        for coarse_id in coarse_ids {
            let Some(l1) = self.l1.get(&coarse_id) else {
                continue;
            };
            let fine_m = self.l0.probe_fine_count(l1);
            for local in l1.nearest_fine(query, self.l0.distance_metric, fine_m) {
                fine_ids.push(self.l0.global_fine_id(coarse_id, local));
            }
        }
        fine_ids.sort_unstable();
        fine_ids.dedup();
        fine_ids
    }

    pub fn candidate_doc_ids(&self, query: &[f64]) -> HashSet<String> {
        let mut ids = HashSet::new();
        for fine_id in self.probe_fine_centroids(query) {
            if let Some(cluster) = self.clusters.get(&fine_id) {
                for m in &cluster.members {
                    ids.insert(m.doc_id.clone());
                }
            }
        }
        ids
    }

    pub fn query_ann(&self, query: &[f64], top_k: usize) -> Vec<(String, f64)> {
        if query.len() != self.l0.dimensions as usize {
            return Vec::new();
        }
        let metric = self.l0.distance_metric;
        let mut scores: HashMap<String, f64> = HashMap::new();
        for fine_id in self.probe_fine_centroids(query) {
            let Some(cluster) = self.clusters.get(&fine_id) else {
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

    /// S3 keys for all L1 segments.
    pub fn all_l1_keys(&self, namespace: &str) -> Vec<String> {
        (0..self.l0.num_coarse)
            .map(|c| CentroidIndexL1::key(namespace, c))
            .collect()
    }

    /// S3 keys for all cluster segments.
    pub fn all_cluster_keys(&self, namespace: &str) -> Vec<String> {
        (0..self.l0.num_fine_total)
            .map(|fid| ClusterSegment::key(namespace, fid))
            .collect()
    }
}

fn num_coarse(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    if n <= 32 {
        return 1;
    }
    let sqrt_k = (n as f64).sqrt().ceil() as usize;
    let k = (sqrt_k / 4).max(4);
    k.clamp(1, n).min(MAX_COARSE_CENTROIDS)
}

fn num_fine(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let sqrt_k = (n as f64).sqrt().ceil() as usize;
    sqrt_k.clamp(1, n).min(MAX_FINE_PER_COARSE)
}

fn kmeans_centroids(pairs: &[(String, Vec<f64>)], k: usize, dim: usize) -> Vec<Vec<f64>> {
    let n = pairs.len();
    if n == 0 {
        return Vec::new();
    }
    if k >= n {
        return pairs.iter().map(|(_, v)| v.clone()).collect();
    }

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

pub fn extract_vector(attrs: &HashMap<String, Value>, field: &str) -> Result<Vec<f64>> {
    let v = attrs
        .get(field)
        .ok_or_else(|| anyhow::anyhow!("missing vector field {field}"))?;
    value_to_f64_vec(v)
}

pub fn value_to_f64_vec(v: &Value) -> Result<Vec<f64>> {
    crate::vector_encoding::decode_vector_value(v)
}

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
    crate::schema::field_is_vector_spec(spec)
}

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

/// Brute-force top-k for recall tests.
pub fn brute_force_top_k(
    docs: &[(String, Vec<f64>)],
    query: &[f64],
    metric: DistanceMetric,
    top_k: usize,
) -> Vec<String> {
    let mut scored: Vec<(String, f64)> = docs
        .iter()
        .map(|(id, v)| (id.clone(), score_vector(query, v, metric)))
        .collect();
    scored.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    scored.truncate(top_k);
    scored.into_iter().map(|(id, _)| id).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Document;
    use serde_json::json;
    use std::collections::HashSet;

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
    fn recall_at_10_1k_docs_32dim_above_point_seven() {
        const DIM: usize = 32;
        const N: usize = 1000;
        const TOP_K: usize = 10;
        const QUERIES: usize = 20;

        let mut docs = Vec::new();
        let mut vectors: Vec<(String, Vec<f64>)> = Vec::new();
        for i in 0..N {
            let mut v = vec![0.0f64; DIM];
            for d in 0..DIM {
                let seed = (i as u64).wrapping_mul(1_103_515_245).wrapping_add(d as u64);
                v[d] = ((seed % 10_000) as f64) / 10_000.0;
            }
            let norm: f64 = v.iter().map(|x| x * x).sum::<f64>().sqrt();
            if norm > 0.0 {
                for x in &mut v {
                    *x /= norm;
                }
            }
            let id = format!("doc-{i}");
            vectors.push((id.clone(), v.clone()));
            docs.push(vec_doc(&id, v));
        }

        let index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
        )
        .unwrap()
        .expect("index built");

        assert!(index.l0.num_coarse > 1, "expected hierarchical coarse level");
        assert!(index.l0.num_fine_total > index.l0.num_coarse);

        let metric = DistanceMetric::CosineDistance;
        let mut recall_sum = 0.0f64;
        for q in 0..QUERIES {
            let query = vectors[q * (N / QUERIES)].1.clone();
            let brute = brute_force_top_k(&vectors, &query, metric, TOP_K);
            let ann = index.query_ann(&query, TOP_K);
            let ann_set: HashSet<_> = ann.into_iter().map(|(id, _)| id).collect();
            let hits = brute.iter().filter(|id| ann_set.contains(*id)).count();
            recall_sum += hits as f64 / TOP_K as f64;
        }
        let recall = recall_sum / QUERIES as f64;
        assert!(
            recall > 0.7,
            "recall@10 {recall} should exceed 0.7 vs brute force"
        );
    }

    #[test]
    fn centroid_l0_roundtrip_bincode() {
        let idx = CentroidIndexL0 {
            segment_id: 3,
            vector_field: "emb".into(),
            dimensions: 2,
            num_coarse: 2,
            num_fine_total: 4,
            probe_coarse: 2,
            probe_fine: 2,
            distance_metric: DistanceMetric::CosineDistance,
            fine_counts: vec![2, 2],
            centroids: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
        };
        let bytes = idx.encode().unwrap();
        let back = CentroidIndexL0::decode(&bytes).unwrap();
        assert_eq!(back.segment_id, 3);
        assert_eq!(back.fine_counts, vec![2, 2]);
    }

    #[test]
    fn apply_delta_adds_doc_to_nearest_cluster_without_rebuild() {
        let docs = vec![
            vec_doc("a", vec![1.0, 0.0]),
            vec_doc("b", vec![0.0, 1.0]),
        ];
        let mut index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
        )
        .unwrap()
        .expect("index");
        let before_count = index.doc_count();
        let new_doc = vec_doc("near-a", vec![0.99, 0.01]);
        index.apply_delta(&[new_doc], &[]).expect("apply_delta");
        assert_eq!(index.doc_count(), before_count + 1);
        assert!(!index.needs_full_rebuild());
    }

    #[test]
    fn needs_full_rebuild_when_docs_exceed_multiplier() {
        let mut docs = Vec::new();
        for i in 0..25 {
            docs.push(vec_doc(
                &format!("d{i}"),
                vec![(i as f64) * 0.1, 1.0 - (i as f64) * 0.1],
            ));
        }
        let index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
        )
        .unwrap()
        .expect("index");
        let k = index.l0.num_fine_total as usize;
        assert!(
            index.doc_count() > k.saturating_mul(REBUILD_DOC_MULTIPLIER),
            "test setup: doc_count {} should exceed {} * {}",
            index.doc_count(),
            k,
            REBUILD_DOC_MULTIPLIER
        );
        assert!(index.needs_full_rebuild());
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

    #[test]
    fn l1_key_format() {
        assert!(CentroidIndexL1::key("ns", 3).contains("centroids-l1-00000003"));
    }
}