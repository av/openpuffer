//! Vector ANN index: SPFresh-style two-level centroid / cluster layout on S3.
//!
//! Layout (per vector column `field`, max 2 per namespace):
//! - `openpuffer/{ns}/index/{field}/centroids-l0.bin` — coarse centroid table + metadata
//! - `openpuffer/{ns}/index/{field}/centroids-l1-{coarse_id:08}.bin` — fine centroids per coarse cell
//! - `openpuffer/{ns}/index/{field}/clusters-{fine_id:08}.bin` — doc ids + vectors per fine centroid
//!
//! Legacy single-vector namespaces may still use `index/centroids-l0.bin` (no field prefix).

use crate::config::{AnnBuildConfig, AnnProbeConfig};
use crate::meta::DistanceMetric;
use crate::models::Document;
use crate::schema::{vector_element_for_field, VectorElement};
use crate::vector_encoding::{f16_le_bytes_to_f32_vec, f64_slice_to_f16_le};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// How many nearest coarse centroids to probe at query time.
pub const DEFAULT_PROBE_COARSE: u32 = 4;

/// Fine centroids to probe per selected coarse cell.
pub const DEFAULT_PROBE_FINE: u32 = 2;

/// On-disk ANN layout version (v2 = legacy two-level cap at 16 coarse).
pub const ANN_VERSION_V2: u8 = 2;

/// SPFresh-style v3: scalable coarse count, optional L2 routing splits.
pub const ANN_VERSION_V3: u8 = 3;

/// Max coarse centroids for v2 builds (level 0).
pub const MAX_COARSE_CENTROIDS: usize = 16;

/// Max coarse centroids for v3 builds.
pub const MAX_COARSE_CENTROIDS_V3: usize = 256;

/// Max fine centroids per coarse cell (v2).
const MAX_FINE_PER_COARSE: usize = 256;

/// Max fine centroids per coarse cell (v3); keeps index object count bounded at 100k scale.
const MAX_FINE_PER_COARSE_V3: usize = 8;

/// When a coarse cell would exceed this many fine centroids, v3 emits L2 routing splits.
pub const L2_SPLIT_FINE_THRESHOLD: u32 = 32;

/// Target docs per coarse bucket when sizing v3 hierarchy.
const V3_TARGET_DOCS_PER_COARSE: usize = 500;

/// Phase B cap: L1 segment keys + cluster keys per vector field @ 100k scale.
pub const V3_INDEX_OBJECT_CAP: usize = 500;

/// Resolve ANN layout version from `OPENPUFFER_ANN_VERSION` (only `3` selects v3).
pub fn ann_version_from_env() -> u8 {
    match std::env::var("OPENPUFFER_ANN_VERSION")
        .ok()
        .as_deref()
        .map(str::trim)
    {
        Some("3") => ANN_VERSION_V3,
        _ => ANN_VERSION_V2,
    }
}

/// k-means iterations when building.
const KMEANS_ITERS: usize = 10;

/// Legacy doc-count rebuild hint (superseded by incremental maintenance + scheduled rebuild).
pub const REBUILD_DOC_MULTIPLIER: usize = 4;

/// Split a cluster when it exceeds this many members (SPFresh incremental maintenance).
pub const MAX_CLUSTER_MEMBERS: usize = 512;

/// Merge clusters with at most this many members into the nearest sibling fine centroid.
pub const MERGE_CLUSTER_MAX_MEMBERS: usize = 2;

/// Full hierarchy rebuild after this many maintenance passes (scheduled, not doc-count-only).
pub const REBUILD_SCHEDULED_MAINTENANCE_PASSES: u32 = 256;

/// One document vector stored in a cluster segment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterMember {
    pub doc_id: String,
    /// Full-precision vectors when `CentroidIndexL0.vector_element` is f32 (legacy segments).
    #[serde(default)]
    pub vector: Vec<f64>,
    /// Little-endian f16 payload when index uses `[N]f16` schema (half the bytes on S3).
    #[serde(default)]
    pub vector_f16: Option<Vec<u8>>,
}

impl ClusterMember {
    pub fn from_values(doc_id: String, values: &[f64], element: VectorElement) -> Self {
        match element {
            VectorElement::F32 => Self {
                doc_id,
                vector: values.to_vec(),
                vector_f16: None,
            },
            VectorElement::F16 => Self {
                doc_id,
                vector: Vec::new(),
                vector_f16: Some(f64_slice_to_f16_le(values)),
            },
        }
    }

    /// f32 slice for ANN scoring (loads f16 from storage when present).
    pub fn as_f32_slice(&self, dim: usize) -> Vec<f32> {
        if let Some(ref bytes) = self.vector_f16 {
            return f16_le_bytes_to_f32_vec(bytes, dim);
        }
        self.vector.iter().take(dim).map(|&x| x as f32).collect()
    }
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
    /// Cluster member storage: f32 arrays vs packed f16 (`[N]f16` schema).
    #[serde(default)]
    pub vector_element: VectorElement,
    /// Fine centroid count per coarse bucket (defines global fine id offsets).
    pub fine_counts: Vec<u32>,
    pub centroids: Vec<Vec<f64>>,
    /// Layout version: `ANN_VERSION_V2` (default) or `ANN_VERSION_V3`. Appended for dual-read.
    #[serde(default = "default_ann_version")]
    pub ann_version: u8,
    /// When true, `centroids-routing.bin` and optional `centroids-l2-*.bin` exist for this field.
    #[serde(default)]
    pub has_routing: bool,
    /// Incremental maintenance passes since last full rebuild (drives scheduled rebuild).
    #[serde(default)]
    pub maintenance_passes: u32,
}

fn default_ann_version() -> u8 {
    ANN_VERSION_V2
}

/// Pre–v3 on-disk L0 (no trailing `ann_version` / `has_routing` fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CentroidIndexL0Legacy {
    pub segment_id: u64,
    pub vector_field: String,
    pub dimensions: u32,
    pub num_coarse: u32,
    pub num_fine_total: u32,
    pub probe_coarse: u32,
    pub probe_fine: u32,
    pub distance_metric: DistanceMetric,
    #[serde(default)]
    pub vector_element: VectorElement,
    pub fine_counts: Vec<u32>,
    pub centroids: Vec<Vec<f64>>,
}

impl From<CentroidIndexL0Legacy> for CentroidIndexL0 {
    fn from(legacy: CentroidIndexL0Legacy) -> Self {
        Self {
            segment_id: legacy.segment_id,
            vector_field: legacy.vector_field,
            dimensions: legacy.dimensions,
            num_coarse: legacy.num_coarse,
            num_fine_total: legacy.num_fine_total,
            probe_coarse: legacy.probe_coarse,
            probe_fine: legacy.probe_fine,
            distance_metric: legacy.distance_metric,
            vector_element: legacy.vector_element,
            fine_counts: legacy.fine_counts,
            centroids: legacy.centroids,
            ann_version: ANN_VERSION_V2,
            has_routing: false,
            maintenance_passes: 0,
        }
    }
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
            vector_element: VectorElement::default(),
            fine_counts: Vec::new(),
            centroids: Vec::new(),
            ann_version: ANN_VERSION_V2,
            has_routing: false,
            maintenance_passes: 0,
        }
    }
}

/// Optional v3 routing table (`centroids-routing.bin`): L2 split counts per coarse cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CentroidRouting {
    pub ann_version: u8,
    pub segment_id: u64,
    pub vector_field: String,
    pub dimensions: u32,
    /// L2 partition count per coarse id (0 or 1 = no L2 object for that coarse).
    pub l2_counts: Vec<u32>,
}

impl CentroidRouting {
    pub fn key(namespace: &str, field: &str) -> String {
        format!(
            "{}centroids-routing.bin",
            vector_index_prefix(namespace, field)
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context("encode CentroidRouting")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context("decode CentroidRouting")
    }
}

/// Level-2 routing within one coarse cell (`centroids-l2-{coarse_id:08}-{l2_id:08}.bin`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CentroidIndexL2 {
    pub segment_id: u64,
    pub coarse_id: u32,
    pub l2_id: u32,
    /// Global fine id of the first centroid in this L2 partition.
    pub global_fine_start: u32,
    pub num_fine: u32,
    pub centroids: Vec<Vec<f64>>,
}

impl CentroidIndexL2 {
    pub fn key(namespace: &str, field: &str, coarse_id: u32, l2_id: u32) -> String {
        format!(
            "{}centroids-l2-{coarse_id:08}-{l2_id:08}.bin",
            vector_index_prefix(namespace, field)
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context("encode CentroidIndexL2")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context("decode CentroidIndexL2")
    }
}

/// S3 prefix for one vector column's index tree (`index/` or `index/{field}/`).
pub fn vector_index_prefix(namespace: &str, field: &str) -> String {
    if field.is_empty() {
        format!("{}{namespace}/index/", crate::models::ROOT_PREFIX)
    } else {
        format!(
            "{}{namespace}/index/{field}/",
            crate::models::ROOT_PREFIX
        )
    }
}

impl CentroidIndexL0 {
    pub fn key(namespace: &str, field: &str) -> String {
        format!("{}centroids-l0.bin", vector_index_prefix(namespace, field))
    }

    /// Pre–multi-column layout (`index/centroids-l0.bin`).
    pub fn legacy_key(namespace: &str) -> String {
        Self::key(namespace, "")
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context("encode CentroidIndexL0")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if let Ok(l0) = bincode::deserialize::<Self>(bytes) {
            return Ok(l0);
        }
        let legacy: CentroidIndexL0Legacy =
            bincode::deserialize(bytes).context("decode CentroidIndexL0 legacy v2")?;
        Ok(legacy.into())
    }

    pub fn is_v3(&self) -> bool {
        self.ann_version >= ANN_VERSION_V3
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
    pub fn key(namespace: &str, field: &str, coarse_id: u32) -> String {
        format!(
            "{}centroids-l1-{coarse_id:08}.bin",
            vector_index_prefix(namespace, field)
        )
    }

    pub fn legacy_key(namespace: &str, coarse_id: u32) -> String {
        Self::key(namespace, "", coarse_id)
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
    pub fn key(namespace: &str, field: &str, fine_id: u32) -> String {
        format!(
            "{}clusters-{fine_id:08}.bin",
            vector_index_prefix(namespace, field)
        )
    }

    pub fn legacy_key(namespace: &str, fine_id: u32) -> String {
        Self::key(namespace, "", fine_id)
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
        dim: usize,
        element: VectorElement,
    ) -> Vec<(String, f64)> {
        let mut scored: Vec<(String, f64)> = match element {
            VectorElement::F32 => self
                .members
                .iter()
                .map(|m| (m.doc_id.clone(), score_vector(query, &m.vector, metric)))
                .collect(),
            VectorElement::F16 => {
                let query_f32: Vec<f32> = query.iter().take(dim).map(|&x| x as f32).collect();
                self.members
                    .iter()
                    .map(|m| {
                        let cand = m.as_f32_slice(dim);
                        (
                            m.doc_id.clone(),
                            score_vector_f32(&query_f32, &cand, metric) as f64,
                        )
                    })
                    .collect()
            }
        };
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scored.truncate(top_k);
        scored
    }
}

/// In-memory vector index (L0 + L1 segments + cluster segments; v3 may add routing + L2).
#[derive(Debug, Clone, Default)]
pub struct VectorIndex {
    pub l0: CentroidIndexL0,
    pub l1: HashMap<u32, CentroidIndexL1>,
    pub clusters: HashMap<u32, ClusterSegment>,
    /// Present when `l0.has_routing` (v3 optional L2 splits).
    pub routing: Option<CentroidRouting>,
    /// `(coarse_id, l2_id)` → L2 routing segment.
    pub l2: HashMap<(u32, u32), CentroidIndexL2>,
}

impl VectorIndex {
    pub fn build(
        segment_id: u64,
        field: &str,
        metric: DistanceMetric,
        docs: &[(String, Document)],
        schema: &Value,
        build: AnnBuildConfig,
    ) -> Result<Option<Self>> {
        let ann_version = build.ann_version;
        let probes = build.probes;
        let vector_element = vector_element_for_field(schema, field);
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

        let k_coarse = num_coarse_for_version(pairs.len(), ann_version);
        let coarse_vecs = kmeans_centroids(&pairs, k_coarse, dimensions as usize, metric);
        let coarse_assign = assign_to_centroids(&pairs, &coarse_vecs, metric);

        let mut by_coarse: HashMap<u32, Vec<(String, Vec<f64>)>> = HashMap::new();
        for (doc_id, vec) in pairs {
            let coarse = coarse_assign.get(&doc_id).copied().unwrap_or(0);
            by_coarse.entry(coarse).or_default().push((doc_id, vec));
        }

        let mut l1_map: HashMap<u32, CentroidIndexL1> = HashMap::new();
        let mut clusters: HashMap<u32, ClusterSegment> = HashMap::new();
        let mut l2_map: HashMap<(u32, u32), CentroidIndexL2> = HashMap::new();
        let mut l2_counts: Vec<u32> = vec![0; k_coarse];
        let mut fine_counts: Vec<u32> = vec![0; k_coarse];
        let mut global_start = 0u32;
        let use_v3 = ann_version >= ANN_VERSION_V3;

        for coarse_id in 0..k_coarse as u32 {
            let cell_docs = by_coarse.remove(&coarse_id).unwrap_or_default();
            let k_fine = num_fine_for_version(cell_docs.len(), ann_version);
            fine_counts[coarse_id as usize] = k_fine as u32;

            let fine_vecs = if cell_docs.is_empty() {
                Vec::new()
            } else {
                kmeans_centroids(&cell_docs, k_fine, dimensions as usize, metric)
            };

            let fine_assign = assign_to_centroids(&cell_docs, &fine_vecs, metric);

            l1_map.insert(
                coarse_id,
                CentroidIndexL1 {
                    segment_id,
                    coarse_id,
                    global_id_start: global_start,
                    num_fine: fine_vecs.len() as u32,
                    centroids: fine_vecs.clone(),
                },
            );

            if use_v3 && k_fine as u32 > L2_SPLIT_FINE_THRESHOLD {
                let l2_parts = k_fine.div_ceil(L2_SPLIT_FINE_THRESHOLD as usize) as u32;
                l2_counts[coarse_id as usize] = l2_parts;
                let chunk = (k_fine as u32).div_ceil(l2_parts).max(1) as usize;
                for l2_id in 0..l2_parts {
                    let start = (l2_id as usize).saturating_mul(chunk);
                    let end = ((l2_id + 1) as usize).saturating_mul(chunk).min(k_fine);
                    let slice = fine_vecs[start..end].to_vec();
                    if slice.is_empty() {
                        continue;
                    }
                    l2_map.insert(
                        (coarse_id, l2_id),
                        CentroidIndexL2 {
                            segment_id,
                            coarse_id,
                            l2_id,
                            global_fine_start: global_start + start as u32,
                            num_fine: slice.len() as u32,
                            centroids: slice,
                        },
                    );
                }
            }

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
                    .push(ClusterMember::from_values(doc_id, &vec, vector_element));
            }

            global_start += k_fine as u32;
        }

        let num_fine_total = global_start;
        let has_routing = use_v3 && l2_counts.iter().any(|&c| c > 1);
        let routing = if has_routing {
            Some(CentroidRouting {
                ann_version: ANN_VERSION_V3,
                segment_id,
                vector_field: field.to_string(),
                dimensions,
                l2_counts: l2_counts.clone(),
            })
        } else {
            None
        };

        let l0 = CentroidIndexL0 {
            segment_id,
            vector_field: field.to_string(),
            dimensions,
            num_coarse: k_coarse as u32,
            num_fine_total,
            probe_coarse: probes.coarse.min(k_coarse as u32).max(1),
            probe_fine: probes.fine.max(1),
            distance_metric: metric,
            vector_element,
            fine_counts,
            centroids: coarse_vecs,
            ann_version: if use_v3 {
                ANN_VERSION_V3
            } else {
                ANN_VERSION_V2
            },
            has_routing,
            maintenance_passes: 0,
        };

        Ok(Some(VectorIndex {
            l0,
            l1: l1_map,
            clusters,
            routing,
            l2: l2_map,
        }))
    }

    pub fn doc_count(&self) -> usize {
        self.clusters.values().map(|c| c.members.len()).sum()
    }

    /// Largest cluster size (for maintenance / spec tests).
    pub fn max_cluster_members(&self) -> usize {
        self.clusters
            .values()
            .map(|c| c.members.len())
            .max()
            .unwrap_or(0)
    }

    pub fn needs_full_rebuild(&self) -> bool {
        let n = self.doc_count();
        let k = self.l0.num_fine_total as usize;
        if k == 0 || n == 0 {
            return true;
        }
        if self.l0.maintenance_passes >= REBUILD_SCHEDULED_MAINTENANCE_PASSES {
            return true;
        }
        // Fallback when incremental maintenance cannot add fine centroids (all coarse cells full).
        let hierarchy_full = self.l0.fine_counts.iter().all(|&c| {
            c as usize >= self.max_fine_per_coarse()
        });
        hierarchy_full && n > k.saturating_mul(REBUILD_DOC_MULTIPLIER)
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
            self.assign_doc_to_nearest(id.clone(), &vec)?;
        }
        self.run_incremental_maintenance()?;
        Ok(())
    }

    fn assign_doc_to_nearest(&mut self, doc_id: String, vec: &[f64]) -> Result<()> {
        let coarse = self
            .l0
            .nearest_coarse(vec, 1)
            .first()
            .copied()
            .unwrap_or(0);
        let l1 = self
            .l1
            .get(&coarse)
            .ok_or_else(|| anyhow::anyhow!("missing L1 segment for coarse {coarse}"))?;
        let local_fine = l1
            .nearest_fine(vec, self.l0.distance_metric, 1)
            .first()
            .copied()
            .unwrap_or(0);
        let fine_id = self.l0.global_fine_id(coarse, local_fine);
        let element = self.l0.vector_element;
        self.clusters
            .entry(fine_id)
            .or_insert_with(|| ClusterSegment {
                segment_id: self.l0.segment_id,
                centroid_id: fine_id,
                members: Vec::new(),
            })
            .members
            .push(ClusterMember::from_values(doc_id, vec, element));
        Ok(())
    }

    /// SPFresh-style incremental pass: split/merge/reassign clusters; refresh centroids.
    pub fn run_incremental_maintenance(&mut self) -> Result<()> {
        if self.l0.num_fine_total == 0 || self.l0.centroids.is_empty() {
            return Ok(());
        }
        self.l0.maintenance_passes = self.l0.maintenance_passes.saturating_add(1);
        self.prune_empty_clusters();
        self.split_oversized_clusters()?;
        self.merge_tiny_clusters();
        self.reassign_drifted_members();
        self.refresh_fine_centroids();
        self.prune_empty_clusters();
        Ok(())
    }

    fn max_fine_per_coarse(&self) -> usize {
        if self.l0.is_v3() {
            MAX_FINE_PER_COARSE_V3
        } else {
            MAX_FINE_PER_COARSE
        }
    }

    fn coarse_local_from_global(&self, fine_id: u32) -> Option<(u32, u32)> {
        let mut offset = 0u32;
        for (coarse_id, &count) in self.l0.fine_counts.iter().enumerate() {
            if fine_id < offset.saturating_add(count) {
                return Some((coarse_id as u32, fine_id - offset));
            }
            offset = offset.saturating_add(count);
        }
        None
    }

    fn prune_empty_clusters(&mut self) {
        self.clusters.retain(|_, c| !c.members.is_empty());
    }

    fn split_oversized_clusters(&mut self) -> Result<()> {
        let dim = self.l0.dimensions as usize;
        let metric = self.l0.distance_metric;
        let element = self.l0.vector_element;
        let oversized: Vec<u32> = self
            .clusters
            .iter()
            .filter(|(_, c)| c.members.len() > MAX_CLUSTER_MEMBERS)
            .map(|(id, _)| *id)
            .collect();
        for fine_id in oversized {
            let Some((coarse_id, local_fine)) = self.coarse_local_from_global(fine_id) else {
                continue;
            };
            let members = self
                .clusters
                .get(&fine_id)
                .map(|c| c.members.clone())
                .unwrap_or_default();
            if members.len() <= MAX_CLUSTER_MEMBERS {
                continue;
            }
            let pairs = members_as_pairs(&members, dim, element);
            let sub_centroids = kmeans_centroids(&pairs, 2, dim, metric);
            if sub_centroids.len() < 2 {
                continue;
            }
            let assign = assign_to_centroids(&pairs, &sub_centroids, metric);
            let mut buckets: [Vec<ClusterMember>; 2] = [Vec::new(), Vec::new()];
            for m in members {
                let sub = assign
                    .get(&m.doc_id)
                    .copied()
                    .unwrap_or(0)
                    .min(1) as usize;
                buckets[sub].push(m);
            }
            if buckets[0].is_empty() || buckets[1].is_empty() {
                continue;
            }
            if self.can_add_fine_centroid(coarse_id) {
                let new_local = self.append_fine_centroid(coarse_id, sub_centroids[1].clone())?;
                let new_global = self.l0.global_fine_id(coarse_id, new_local);
                if let Some(l1) = self.l1.get_mut(&coarse_id) {
                    if (local_fine as usize) < l1.centroids.len() {
                        l1.centroids[local_fine as usize] = sub_centroids[0].clone();
                    }
                }
                if let Some(cluster) = self.clusters.get_mut(&fine_id) {
                    cluster.members = buckets[0].clone();
                    cluster.centroid_id = fine_id;
                }
                self.clusters.insert(
                    new_global,
                    ClusterSegment {
                        segment_id: self.l0.segment_id,
                        centroid_id: new_global,
                        members: buckets[1].clone(),
                    },
                );
            } else {
                self.redistribute_to_existing_fines(coarse_id, fine_id, &buckets[0], &buckets[1])?;
            }
        }
        Ok(())
    }

    fn can_add_fine_centroid(&self, coarse_id: u32) -> bool {
        let max = self.max_fine_per_coarse();
        self.l0
            .fine_counts
            .get(coarse_id as usize)
            .map(|&c| c as usize)
            .unwrap_or(0)
            < max
    }

    /// Append a fine centroid at the end of `coarse_id`; shifts global ids for later coarse cells.
    fn append_fine_centroid(&mut self, coarse_id: u32, centroid: Vec<f64>) -> Result<u32> {
        let l1 = self
            .l1
            .get_mut(&coarse_id)
            .ok_or_else(|| anyhow::anyhow!("missing L1 for coarse {coarse_id}"))?;
        let new_local = l1.num_fine;
        l1.centroids.push(centroid);
        l1.num_fine = l1.centroids.len() as u32;

        let pivot = self.l0.global_id_start(coarse_id) + new_local;
        let mut old_ids: Vec<u32> = self
            .clusters
            .keys()
            .copied()
            .filter(|fid| *fid >= pivot)
            .collect();
        old_ids.sort_unstable_by(|a, b| b.cmp(a));
        let mut moved = Vec::new();
        for old_id in old_ids {
            if let Some(mut seg) = self.clusters.remove(&old_id) {
                let new_id = old_id + 1;
                seg.centroid_id = new_id;
                moved.push((new_id, seg));
            }
        }
        for (new_id, seg) in moved {
            self.clusters.insert(new_id, seg);
        }

        let idx = coarse_id as usize;
        if idx < self.l0.fine_counts.len() {
            self.l0.fine_counts[idx] += 1;
        }
        self.l0.num_fine_total += 1;
        for c in (coarse_id + 1)..self.l0.num_coarse {
            if let Some(l1) = self.l1.get_mut(&c) {
                l1.global_id_start += 1;
            }
        }
        Ok(new_local)
    }

    fn redistribute_to_existing_fines(
        &mut self,
        coarse_id: u32,
        source_fine: u32,
        bucket0: &[ClusterMember],
        bucket1: &[ClusterMember],
    ) -> Result<()> {
        let dim = self.l0.dimensions as usize;
        let metric = self.l0.distance_metric;
        let l1 = self
            .l1
            .get(&coarse_id)
            .ok_or_else(|| anyhow::anyhow!("missing L1 for coarse {coarse_id}"))?;
        let mut target_local = 0u32;
        let mut best_score = f64::NEG_INFINITY;
        for local in 0..l1.num_fine {
            let global = self.l0.global_fine_id(coarse_id, local);
            if global == source_fine {
                continue;
            }
            if let Some(c) = l1.centroids.get(local as usize) {
                let probe = member_as_f64(&bucket1[0], dim);
                let s = score_vector(&probe, c, metric);
                if s > best_score {
                    best_score = s;
                    target_local = local;
                }
            }
        }
        let target_global = self.l0.global_fine_id(coarse_id, target_local);
        if let Some(cluster) = self.clusters.get_mut(&source_fine) {
            cluster.members = bucket0.to_vec();
        }
        self.clusters
            .entry(target_global)
            .or_insert_with(|| ClusterSegment {
                segment_id: self.l0.segment_id,
                centroid_id: target_global,
                members: Vec::new(),
            })
            .members
            .extend_from_slice(bucket1);
        Ok(())
    }

    fn merge_tiny_clusters(&mut self) {
        let tiny: Vec<u32> = self
            .clusters
            .iter()
            .filter(|(_, c)| {
                let n = c.members.len();
                n > 0 && n <= MERGE_CLUSTER_MAX_MEMBERS
            })
            .map(|(id, _)| *id)
            .collect();
        for fine_id in tiny {
            let Some((coarse_id, _local)) = self.coarse_local_from_global(fine_id) else {
                continue;
            };
            let members = self
                .clusters
                .remove(&fine_id)
                .map(|c| c.members)
                .unwrap_or_default();
            if members.is_empty() {
                continue;
            }
            let dim = self.l0.dimensions as usize;
            let metric = self.l0.distance_metric;
            let sample = member_as_f64(&members[0], dim);
            let l1 = match self.l1.get(&coarse_id) {
                Some(l) => l,
                None => continue,
            };
            let target_local = l1
                .nearest_fine(&sample, metric, 1)
                .first()
                .copied()
                .unwrap_or(0);
            let target_global = self.l0.global_fine_id(coarse_id, target_local);
            if target_global == fine_id {
                self.clusters.insert(
                    fine_id,
                    ClusterSegment {
                        segment_id: self.l0.segment_id,
                        centroid_id: fine_id,
                        members,
                    },
                );
                continue;
            }
            self.clusters
                .entry(target_global)
                .or_insert_with(|| ClusterSegment {
                    segment_id: self.l0.segment_id,
                    centroid_id: target_global,
                    members: Vec::new(),
                })
                .members
                .extend(members);
        }
    }

    fn reassign_drifted_members(&mut self) {
        let dim = self.l0.dimensions as usize;
        let metric = self.l0.distance_metric;
        let mut moves: Vec<(String, u32)> = Vec::new();
        for (fine_id, cluster) in &self.clusters {
            let Some((coarse_id, current_local)) = self.coarse_local_from_global(*fine_id) else {
                continue;
            };
            let l1 = match self.l1.get(&coarse_id) {
                Some(l) => l,
                None => continue,
            };
            for m in &cluster.members {
                let v = member_as_f64(m, dim);
                let best_local = l1
                    .nearest_fine(&v, metric, 1)
                    .first()
                    .copied()
                    .unwrap_or(current_local);
                if best_local != current_local {
                    let dest = self.l0.global_fine_id(coarse_id, best_local);
                    moves.push((m.doc_id.clone(), dest));
                }
            }
        }
        for (doc_id, dest) in moves {
            let mut member = None;
            for cluster in self.clusters.values_mut() {
                if let Some(pos) = cluster.members.iter().position(|m| m.doc_id == doc_id) {
                    member = Some(cluster.members.remove(pos));
                    break;
                }
            }
            if let Some(m) = member {
                self.clusters
                    .entry(dest)
                    .or_insert_with(|| ClusterSegment {
                        segment_id: self.l0.segment_id,
                        centroid_id: dest,
                        members: Vec::new(),
                    })
                    .members
                    .push(m);
            }
        }
    }

    fn refresh_fine_centroids(&mut self) {
        let dim = self.l0.dimensions as usize;
        let metric = self.l0.distance_metric;
        for (coarse_id, l1) in self.l1.iter_mut() {
            for local in 0..l1.num_fine {
                let global = self.l0.global_fine_id(*coarse_id, local);
                let Some(cluster) = self.clusters.get(&global) else {
                    continue;
                };
                if cluster.members.is_empty() {
                    continue;
                }
                let pairs = members_as_pairs(
                    &cluster.members,
                    dim,
                    self.l0.vector_element,
                );
                let centroids = kmeans_centroids(&pairs, 1, dim, metric);
                if let Some(c) = centroids.into_iter().next() {
                    if (local as usize) < l1.centroids.len() {
                        l1.centroids[local as usize] = c;
                    }
                }
            }
        }
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

    /// ANN candidate pool: all doc ids in probed clusters (input to exact re-rank).
    pub fn ann_pool_doc_ids(&self, query: &[f64]) -> HashSet<String> {
        self.candidate_doc_ids(query)
    }

    /// Score the probed ANN pool with exact view vectors and return top-*k* (re-rank pass).
    pub fn rerank_top_k(
        &self,
        query: &[f64],
        top_k: usize,
        view_vector: impl Fn(&str) -> Option<Vec<f64>>,
    ) -> Vec<(String, f64)> {
        if query.len() != self.l0.dimensions as usize {
            return Vec::new();
        }
        let metric = self.l0.distance_metric;
        let mut scored: Vec<(String, f64)> = self
            .ann_pool_doc_ids(query)
            .into_iter()
            .filter_map(|id| {
                let v = view_vector(&id)?;
                if v.len() != query.len() {
                    return None;
                }
                Some((id, score_vector(query, &v, metric)))
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

    pub fn query_ann(&self, query: &[f64], top_k: usize) -> Vec<(String, f64)> {
        if query.len() != self.l0.dimensions as usize {
            return Vec::new();
        }
        let metric = self.l0.distance_metric;
        let dim = self.l0.dimensions as usize;
        let element = self.l0.vector_element;
        let mut scores: HashMap<String, f64> = HashMap::new();
        for fine_id in self.probe_fine_centroids(query) {
            let Some(cluster) = self.clusters.get(&fine_id) else {
                continue;
            };
            for (id, score) in cluster.score_members(
                query,
                metric,
                top_k.saturating_mul(4),
                dim,
                element,
            ) {
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
        let field = &self.l0.vector_field;
        (0..self.l0.num_coarse)
            .map(|c| CentroidIndexL1::key(namespace, field, c))
            .collect()
    }

    /// S3 keys for all cluster segments.
    pub fn all_cluster_keys(&self, namespace: &str) -> Vec<String> {
        let field = &self.l0.vector_field;
        (0..self.l0.num_fine_total)
            .map(|fid| ClusterSegment::key(namespace, field, fid))
            .collect()
    }

    /// S3 keys for optional v3 routing + L2 segments.
    pub fn all_v3_aux_keys(&self, namespace: &str) -> Vec<String> {
        let field = &self.l0.vector_field;
        let mut keys = Vec::new();
        if self.l0.has_routing {
            keys.push(CentroidRouting::key(namespace, field));
        }
        for ((coarse_id, l2_id), _) in &self.l2 {
            keys.push(CentroidIndexL2::key(namespace, field, *coarse_id, *l2_id));
        }
        keys.sort();
        keys.dedup();
        keys
    }

    /// L1 + cluster + optional v3 aux object count (for benchmark/spec caps).
    pub fn index_object_count(&self) -> usize {
        self.l0.num_coarse as usize
            + self.l0.num_fine_total as usize
            + self.all_v3_aux_keys("").len()
    }
}

/// Vector field names to build ANN indexes for (schema order, max 2).
pub fn vector_fields_to_index(
    schema: &Value,
    meta: &crate::meta::NamespaceMeta,
    sample: Option<&Document>,
) -> Vec<String> {
    let mut fields: Vec<String> = meta
        .vector_fields
        .iter()
        .map(|f| f.name.clone())
        .collect();
    if fields.is_empty() {
        fields = vector_fields_from_schema(schema);
    }
    if fields.len() > crate::meta::MAX_VECTOR_FIELDS {
        fields.truncate(crate::meta::MAX_VECTOR_FIELDS);
    }
    if fields.is_empty() {
        if !meta.vector_field.is_empty() {
            fields.push(meta.vector_field.clone());
        } else if let Some(name) = primary_vector_field(schema, sample) {
            fields.push(name);
        }
    }
    fields
}

fn num_coarse_for_version(n: usize, ann_version: u8) -> usize {
    if n == 0 {
        return 0;
    }
    if ann_version >= ANN_VERSION_V3 {
        return num_coarse_v3(n);
    }
    num_coarse_v2(n)
}

fn num_fine_for_version(n: usize, ann_version: u8) -> usize {
    if n == 0 {
        return 0;
    }
    if ann_version >= ANN_VERSION_V3 {
        return num_fine_v3(n);
    }
    num_fine_v2(n)
}

fn num_coarse_v2(n: usize) -> usize {
    if n <= 32 {
        return 1;
    }
    let sqrt_k = (n as f64).sqrt().ceil() as usize;
    let k = (sqrt_k / 4).max(4);
    k.clamp(1, n).min(MAX_COARSE_CENTROIDS)
}

/// Estimated L1 + cluster object count for a v3 layout (matches `index_object_count` sans aux).
fn v3_l1_cluster_object_count_estimate(n: usize, k_coarse: usize) -> usize {
    if k_coarse == 0 {
        return 0;
    }
    let cell = n.div_ceil(k_coarse);
    let k_fine = num_fine_v3(cell);
    k_coarse + k_coarse.saturating_mul(k_fine)
}

fn num_coarse_v3(n: usize) -> usize {
    if n <= 32 {
        return 1;
    }
    let mut k = (n / V3_TARGET_DOCS_PER_COARSE).max(8);
    k = k.clamp(8, n).min(MAX_COARSE_CENTROIDS_V3);
    while k > 8 && v3_l1_cluster_object_count_estimate(n, k) >= V3_INDEX_OBJECT_CAP {
        k -= 1;
    }
    k
}

fn num_fine_v2(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let sqrt_k = (n as f64).sqrt().ceil() as usize;
    sqrt_k.clamp(1, n).min(MAX_FINE_PER_COARSE)
}

fn num_fine_v3(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let sqrt_k = (n as f64).sqrt().ceil() as usize;
    sqrt_k.clamp(1, n).min(MAX_FINE_PER_COARSE_V3)
}

/// Squared distance from a point to a centroid (for k-means++ weighting).
fn point_dist_sq(vec: &[f64], center: &[f64], metric: DistanceMetric) -> f64 {
    match metric {
        DistanceMetric::CosineDistance => {
            let sim = cosine_similarity(vec, center);
            let d = (1.0 - sim).max(0.0);
            d * d
        }
        DistanceMetric::EuclideanSquared => euclidean_squared(vec, center),
    }
}

/// Deterministic xorshift64 PRNG for reproducible k-means++ center picks.
struct XorShift64(u64);

impl XorShift64 {
    fn seed(n: usize, k: usize, pairs: &[(String, Vec<f64>)]) -> Self {
        let mut s = (n as u64)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(k as u64);
        if let Some((_, v)) = pairs.first() {
            for (i, &x) in v.iter().take(8).enumerate() {
                s = s.wrapping_add((x.to_bits() as u64).wrapping_mul(i as u64 + 1));
            }
        }
        if s == 0 {
            s = 0xA076_1D64_78BD_642F;
        }
        Self(s)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn unit_interval(&mut self) -> f64 {
        const SCALE: f64 = 1.0 / (u64::MAX as f64);
        (self.next_u64() as f64) * SCALE
    }

    fn index(&mut self, upper: usize) -> usize {
        if upper <= 1 {
            return 0;
        }
        (self.next_u64() as usize) % upper
    }
}

/// k-means++ initialization (Arthur & Vassilvitskii, 2007) then Lloyd refinement.
fn kmeans_plus_plus_init(
    pairs: &[(String, Vec<f64>)],
    k: usize,
    metric: DistanceMetric,
) -> Vec<Vec<f64>> {
    let n = pairs.len();
    let mut rng = XorShift64::seed(n, k, pairs);
    let mut centroids: Vec<Vec<f64>> = Vec::with_capacity(k);
    centroids.push(pairs[rng.index(n)].1.clone());

    for _ in 1..k {
        let mut dist_sq = vec![0.0f64; n];
        let mut total = 0.0f64;
        for (i, (_, v)) in pairs.iter().enumerate() {
            let mut min_d = f64::INFINITY;
            for c in &centroids {
                min_d = min_d.min(point_dist_sq(v, c, metric));
            }
            dist_sq[i] = min_d;
            total += min_d;
        }
        let pick = if total <= f64::EPSILON {
            (centroids.len() % n).max(0)
        } else {
            let r = rng.unit_interval() * total;
            let mut acc = 0.0f64;
            let mut idx = n.saturating_sub(1);
            for (i, &d) in dist_sq.iter().enumerate() {
                acc += d;
                if acc >= r {
                    idx = i;
                    break;
                }
            }
            idx
        };
        centroids.push(pairs[pick].1.clone());
    }
    centroids
}

fn kmeans_centroids(
    pairs: &[(String, Vec<f64>)],
    k: usize,
    dim: usize,
    metric: DistanceMetric,
) -> Vec<Vec<f64>> {
    let n = pairs.len();
    if n == 0 {
        return Vec::new();
    }
    if k >= n {
        return pairs.iter().map(|(_, v)| v.clone()).collect();
    }

    let mut centroids = kmeans_plus_plus_init(pairs, k, metric);

    for _ in 0..KMEANS_ITERS {
        let mut sums: Vec<Vec<f64>> = vec![vec![0.0; dim]; k];
        let mut counts = vec![0usize; k];

        for (_, v) in pairs {
            let best = nearest_centroid_id(v, &centroids, metric);
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

/// f32 ANN scoring (used when cluster vectors are stored as f16).
pub fn score_vector_f32(query: &[f32], candidate: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::CosineDistance => cosine_similarity_f32(query, candidate),
        DistanceMetric::EuclideanSquared => {
            let d2 = euclidean_squared_f32(query, candidate);
            if d2.is_finite() {
                -d2
            } else {
                f32::NEG_INFINITY
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

pub fn cosine_similarity_f32(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
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

fn euclidean_squared_f32(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return f32::INFINITY;
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

fn member_as_f64(m: &ClusterMember, dim: usize) -> Vec<f64> {
    if let Some(ref bytes) = m.vector_f16 {
        return f16_le_bytes_to_f32_vec(bytes, dim)
            .into_iter()
            .map(|x| x as f64)
            .collect();
    }
    m.vector.clone()
}

fn members_as_pairs(
    members: &[ClusterMember],
    dim: usize,
    _element: VectorElement,
) -> Vec<(String, Vec<f64>)> {
    members
        .iter()
        .map(|m| (m.doc_id.clone(), member_as_f64(m, dim)))
        .collect()
}

/// Mean recall@*k* over queries comparing ANN results to brute-force ground truth.
pub fn measure_recall_at_k(
    index: &VectorIndex,
    vectors: &[(String, Vec<f64>)],
    queries: usize,
    top_k: usize,
    use_rerank: bool,
    view_vector: impl Fn(&str) -> Option<Vec<f64>>,
) -> f64 {
    if queries == 0 || vectors.is_empty() {
        return 0.0;
    }
    let metric = index.l0.distance_metric;
    let n = vectors.len();
    let mut recall_sum = 0.0f64;
    for q in 0..queries {
        let query = vectors[q * (n / queries)].1.clone();
        let brute = brute_force_top_k(vectors, &query, metric, top_k);
        let ann: HashSet<_> = if use_rerank {
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
        let hits = brute.iter().filter(|id| ann.contains(*id)).count();
        recall_sum += hits as f64 / top_k as f64;
    }
    recall_sum / queries as f64
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
            &json!({}),
            AnnBuildConfig::default(),
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
            &json!({ "embedding": format!("[{DIM}]f32") }),
            AnnBuildConfig::default(),
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
            recall > 0.75,
            "recall@10 {recall} should exceed 0.75 vs brute force (k-means++)"
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
            vector_element: VectorElement::F32,
            fine_counts: vec![2, 2],
            centroids: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            ann_version: ANN_VERSION_V2,
            has_routing: false,
            maintenance_passes: 7,
        };
        let bytes = idx.encode().unwrap();
        let back = CentroidIndexL0::decode(&bytes).unwrap();
        assert_eq!(back.segment_id, 3);
        assert_eq!(back.maintenance_passes, 7);
        assert_eq!(back.fine_counts, vec![2, 2]);
        assert_eq!(back.ann_version, ANN_VERSION_V2);
    }

    #[test]
    fn ann_version_v3_roundtrip() {
        let mut docs = Vec::new();
        for i in 0..20_000 {
            let angle = (i as f64) * 0.01;
            docs.push(vec_doc(
                &format!("doc-{i}"),
                vec![angle.cos(), angle.sin(), 0.1, 0.2],
            ));
        }
        let build = AnnBuildConfig::default().with_ann_version(ANN_VERSION_V3);
        let index = VectorIndex::build(
            42,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
            &json!({ "embedding": "[4]f32" }),
            build,
        )
        .unwrap()
        .expect("v3 index");

        assert_eq!(index.l0.ann_version, ANN_VERSION_V3);
        let v2 = VectorIndex::build(
            42,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
            &json!({ "embedding": "[4]f32" }),
            AnnBuildConfig::default().with_ann_version(ANN_VERSION_V2),
        )
        .unwrap()
        .expect("v2 index");
        assert!(
            index.l0.num_coarse > v2.l0.num_coarse,
            "v3 coarse {} should exceed v2 {} at 20k docs",
            index.l0.num_coarse,
            v2.l0.num_coarse
        );

        let l0_bytes = index.l0.encode().unwrap();
        let l0_back = CentroidIndexL0::decode(&l0_bytes).unwrap();
        assert_eq!(l0_back.ann_version, ANN_VERSION_V3);
        assert_eq!(l0_back.has_routing, index.l0.has_routing);

        if let Some(ref routing) = index.routing {
            let r_bytes = routing.encode().unwrap();
            let r_back = CentroidRouting::decode(&r_bytes).unwrap();
            assert_eq!(r_back.ann_version, ANN_VERSION_V3);
            assert_eq!(r_back.l2_counts, routing.l2_counts);
        }

        for ((coarse_id, l2_id), l2) in &index.l2 {
            let bytes = l2.encode().unwrap();
            let back = CentroidIndexL2::decode(&bytes).unwrap();
            assert_eq!(back.coarse_id, *coarse_id);
            assert_eq!(back.l2_id, *l2_id);
            assert_eq!(back.num_fine, l2.num_fine);
        }

        for l1 in index.l1.values() {
            let bytes = l1.encode().unwrap();
            let back = CentroidIndexL1::decode(&bytes).unwrap();
            assert_eq!(back.num_fine, l1.num_fine);
        }

        for cluster in index.clusters.values() {
            let bytes = cluster.encode().unwrap();
            let back = ClusterSegment::decode(&bytes).unwrap();
            assert_eq!(back.members.len(), cluster.members.len());
        }
    }

    #[test]
    fn ann_version_v2_legacy_segment_still_loads() {
        let legacy = CentroidIndexL0Legacy {
            segment_id: 9,
            vector_field: "emb".into(),
            dimensions: 4,
            num_coarse: 4,
            num_fine_total: 16,
            probe_coarse: 4,
            probe_fine: 2,
            distance_metric: DistanceMetric::CosineDistance,
            vector_element: VectorElement::F32,
            fine_counts: vec![4, 4, 4, 4],
            centroids: vec![
                vec![1.0, 0.0, 0.0, 0.0],
                vec![0.0, 1.0, 0.0, 0.0],
                vec![0.0, 0.0, 1.0, 0.0],
                vec![0.0, 0.0, 0.0, 1.0],
            ],
        };
        let bytes = bincode::serialize(&legacy).unwrap();
        let back = CentroidIndexL0::decode(&bytes).unwrap();
        assert_eq!(back.ann_version, ANN_VERSION_V2);
        assert!(!back.has_routing);
        assert_eq!(back.num_coarse, 4);
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
            &json!({ "embedding": "[2]f32" }),
            AnnBuildConfig::default(),
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
    fn needs_full_rebuild_on_scheduled_maintenance_passes() {
        let docs = vec![
            vec_doc("a", vec![1.0, 0.0]),
            vec_doc("b", vec![0.0, 1.0]),
        ];
        let mut index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
            &json!({ "embedding": "[2]f32" }),
            AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("index");
        assert!(!index.needs_full_rebuild());
        index.l0.maintenance_passes = REBUILD_SCHEDULED_MAINTENANCE_PASSES;
        assert!(index.needs_full_rebuild());
    }

    fn synthetic_wave_docs(start: usize, count: usize, dim: usize) -> Vec<(String, Document)> {
        let mut docs = Vec::with_capacity(count);
        for i in 0..count {
            let idx = start + i;
            let mut v = vec![0.0f64; dim];
            for d in 0..dim {
                let seed = (idx as u64)
                    .wrapping_mul(1_103_515_245)
                    .wrapping_add(d as u64);
                v[d] = ((seed % 10_000) as f64) / 10_000.0;
            }
            let norm: f64 = v.iter().map(|x| x * x).sum::<f64>().sqrt();
            if norm > 0.0 {
                for x in &mut v {
                    *x /= norm;
                }
            }
            docs.push(vec_doc(&format!("doc-{idx}"), v));
        }
        docs
    }

    fn recall_at_10(index: &VectorIndex, vectors: &[(String, Vec<f64>)], queries: usize) -> f64 {
        const TOP_K: usize = 10;
        let metric = index.l0.distance_metric;
        let n = vectors.len();
        let mut recall_sum = 0.0f64;
        for q in 0..queries {
            let query = vectors[q * (n / queries)].1.clone();
            let brute = brute_force_top_k(vectors, &query, metric, TOP_K);
            let ann = index.query_ann(&query, TOP_K);
            let ann_set: HashSet<_> = ann.into_iter().map(|(id, _)| id).collect();
            let hits = brute.iter().filter(|id| ann_set.contains(*id)).count();
            recall_sum += hits as f64 / TOP_K as f64;
        }
        recall_sum / queries as f64
    }

    #[test]
    fn spfresh_incremental_four_waves_20k_without_forced_rebuild() {
        const DIM: usize = 32;
        const WAVE: usize = 5000;
        const WAVES: usize = 4;
        const TOP_K: usize = 10;
        const QUERIES: usize = 20;
        const MIN_RECALL: f64 = 0.80;

        let wave0 = synthetic_wave_docs(0, WAVE, DIM);
        let mut vectors: Vec<(String, Vec<f64>)> = wave0
            .iter()
            .map(|(id, d)| {
                (
                    id.clone(),
                    extract_vector(&d.attributes, "embedding").unwrap(),
                )
            })
            .collect();

        let mut index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &wave0,
            &json!({ "embedding": format!("[{DIM}]f32") }),
            AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("initial index");

        for wave in 1..WAVES {
            let start = wave * WAVE;
            let batch = synthetic_wave_docs(start, WAVE, DIM);
            for (id, v) in batch.iter().map(|(id, d)| {
                (
                    id.clone(),
                    extract_vector(&d.attributes, "embedding").unwrap(),
                )
            }) {
                vectors.push((id, v));
            }
            index
                .apply_delta(&batch, &[])
                .expect("apply_delta wave");
            assert!(
                !index.needs_full_rebuild(),
                "wave {wave}: should not force full rebuild (passes={})",
                index.l0.maintenance_passes
            );
            assert!(
                index.max_cluster_members() <= MAX_CLUSTER_MEMBERS,
                "wave {wave}: cluster size {} exceeds {}",
                index.max_cluster_members(),
                MAX_CLUSTER_MEMBERS
            );
            let recall = recall_at_10(&index, &vectors, QUERIES);
            assert!(
                recall >= MIN_RECALL,
                "wave {wave}: recall@10 {recall} below {MIN_RECALL}"
            );
        }

        assert_eq!(index.doc_count(), WAVE * WAVES);
        let recall_final = recall_at_10(&index, &vectors, QUERIES);
        assert!(recall_final >= MIN_RECALL);
        let _ = TOP_K;
    }

    fn bench_synthetic_embedding(doc_index: usize, dim: usize) -> Vec<f64> {
        (0..dim)
            .map(|d| ((doc_index * dim + d) as f64 * 0.001).sin())
            .collect()
    }

    #[test]
    fn recall_at_10_10k_with_rerank_at_least_point_nine_two() {
        const N: usize = 10_000;
        const DIM: usize = 128;
        const TOP_K: usize = 10;
        const QUERIES: usize = 20;
        const MIN_RERANK_RECALL: f64 = 0.92;

        let mut docs = Vec::with_capacity(N);
        let mut vectors: Vec<(String, Vec<f64>)> = Vec::with_capacity(N);
        for i in 0..N {
            let v = bench_synthetic_embedding(i, DIM);
            let id = format!("doc-{i}");
            vectors.push((id.clone(), v.clone()));
            docs.push(vec_doc(&id, v));
        }

        let index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
            &json!({ "embedding": format!("[{DIM}]f32") }),
            AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("10k index built");

        let view = |id: &str| vectors.iter().find(|(d, _)| d == id).map(|(_, v)| v.clone());
        let recall_probe =
            measure_recall_at_k(&index, &vectors, QUERIES, TOP_K, false, view);
        let recall_rerank =
            measure_recall_at_k(&index, &vectors, QUERIES, TOP_K, true, view);

        assert!(
            recall_rerank >= MIN_RERANK_RECALL,
            "re-rank recall@10 {recall_rerank} should be >= {MIN_RERANK_RECALL} on 10k fixture"
        );
        assert!(
            recall_rerank >= recall_probe,
            "re-rank {recall_rerank} should be >= probe-only {recall_probe}"
        );
        let pool_probe = index.query_ann(&vectors[0].1, TOP_K).len();
        let pool_rerank = index.ann_pool_doc_ids(&vectors[0].1).len();
        assert!(
            pool_rerank >= pool_probe,
            "re-rank pool ({pool_rerank}) should be >= probe-only pool ({pool_probe})"
        );
    }

    fn build_10k_synthetic_fixture() -> (Vec<(String, Document)>, Vec<(String, Vec<f64>)>) {
        const N: usize = 10_000;
        const DIM: usize = 128;
        let mut docs = Vec::with_capacity(N);
        let mut vectors: Vec<(String, Vec<f64>)> = Vec::with_capacity(N);
        for i in 0..N {
            let v = bench_synthetic_embedding(i, DIM);
            let id = format!("doc-{i}");
            vectors.push((id.clone(), v.clone()));
            docs.push(vec_doc(&id, v));
        }
        (docs, vectors)
    }

    #[test]
    fn recall_v3_at_least_five_points_above_v2_on_10k_fixture() {
        const QUERIES: usize = 20;
        const TOP_K: usize = 10;
        const MIN_DELTA: f64 = 0.05;
        let schema = json!({ "embedding": "[128]f32" });
        // Tight probes so v2 (16 coarse cap) lags v3 hierarchy on the same 10k fixture.
        let probes = AnnProbeConfig {
            coarse: 2,
            fine: 1,
        };
        let (docs, vectors) = build_10k_synthetic_fixture();

        let v2 = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
            &schema,
            AnnBuildConfig::from_probes(probes).with_ann_version(ANN_VERSION_V2),
        )
        .unwrap()
        .expect("v2 index");
        let v3 = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
            &schema,
            AnnBuildConfig::from_probes(probes).with_ann_version(ANN_VERSION_V3),
        )
        .unwrap()
        .expect("v3 index");

        let view = |id: &str| vectors.iter().find(|(d, _)| d == id).map(|(_, v)| v.clone());
        let recall_v2 = measure_recall_at_k(&v2, &vectors, QUERIES, TOP_K, false, &view);
        let recall_v3 = measure_recall_at_k(&v3, &vectors, QUERIES, TOP_K, false, &view);
        let delta = recall_v3 - recall_v2;
        assert!(
            delta >= MIN_DELTA - 1e-9,
            "v3 recall@10 {recall_v3} should be >= v2 {recall_v2} + {MIN_DELTA} (delta={delta}, same probes)"
        );
    }

    #[test]
    fn ann_v3_index_object_count_100k_under_five_hundred() {
        const N: usize = 100_000;
        let k = num_coarse_v3(N);
        let est = v3_l1_cluster_object_count_estimate(N, k);
        assert!(
            est < V3_INDEX_OBJECT_CAP,
            "v3 100k sizing estimate {est} (coarse={k}) must be < {cap}",
            cap = V3_INDEX_OBJECT_CAP
        );
        // Spot-check built layout matches estimate on 10k (CI-fast).
        const SPOT: usize = 10_000;
        const DIM: usize = 128;
        let mut docs = Vec::with_capacity(SPOT);
        for i in 0..SPOT {
            docs.push(vec_doc(&format!("doc-{i}"), bench_synthetic_embedding(i, DIM)));
        }
        let index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
            &json!({ "embedding": format!("[{DIM}]f32") }),
            AnnBuildConfig::default().with_ann_version(ANN_VERSION_V3),
        )
        .unwrap()
        .expect("v3 10k spot index");
        assert_eq!(index.l0.num_coarse as usize, num_coarse_v3(SPOT));
        assert!(index.index_object_count() < V3_INDEX_OBJECT_CAP);
    }

    #[test]
    #[ignore = "100k in-memory v3 build (~3 min); nightly: cargo test --lib ann_v3_built_index_object_count_100k_under_five_hundred -- --ignored --nocapture"]
    fn ann_v3_built_index_object_count_100k_under_five_hundred() {
        const N: usize = 100_000;
        const DIM: usize = 128;
        let mut docs = Vec::with_capacity(N);
        for i in 0..N {
            docs.push(vec_doc(&format!("doc-{i}"), bench_synthetic_embedding(i, DIM)));
        }
        let index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
            &json!({ "embedding": format!("[{DIM}]f32") }),
            AnnBuildConfig::default().with_ann_version(ANN_VERSION_V3),
        )
        .unwrap()
        .expect("v3 100k index");
        assert!(
            index.index_object_count() < V3_INDEX_OBJECT_CAP,
            "built v3 100k index_object_count {} must be < {}",
            index.index_object_count(),
            V3_INDEX_OBJECT_CAP
        );
    }

    #[test]
    #[ignore = "100k in-memory build + 20 queries (~2–5 min); nightly: cargo test --lib recall_at_10_100k_synthetic_at_least_point_nine -- --ignored --nocapture"]
    fn recall_at_10_100k_synthetic_at_least_point_nine() {
        const N: usize = 100_000;
        const DIM: usize = 128;
        const QUERIES: usize = 20;
        const MIN_RECALL: f64 = 0.90;

        let mut docs = Vec::with_capacity(N);
        let mut vectors: Vec<(String, Vec<f64>)> = Vec::with_capacity(N);
        for i in 0..N {
            let v = bench_synthetic_embedding(i, DIM);
            let id = format!("doc-{i}");
            vectors.push((id.clone(), v.clone()));
            docs.push(vec_doc(&id, v));
        }

        let index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
            &json!({ "embedding": format!("[{DIM}]f32") }),
            AnnBuildConfig::default().with_ann_version(ANN_VERSION_V3),
        )
        .unwrap()
        .expect("100k v3 index");

        let recall = recall_at_10(&index, &vectors, QUERIES);
        assert!(
            recall >= MIN_RECALL,
            "recall@10 {recall} should be >= {MIN_RECALL} on 100k synthetic (v3)"
        );
    }

    #[test]
    fn cluster_segment_roundtrip() {
        let seg = ClusterSegment {
            segment_id: 1,
            centroid_id: 0,
            members: vec![ClusterMember::from_values(
                "a".into(),
                &[1.0, 0.0],
                VectorElement::F32,
            )],
        };
        let bytes = seg.encode().unwrap();
        let back = ClusterSegment::decode(&bytes).unwrap();
        assert_eq!(back.members[0].doc_id, "a");
    }

    #[test]
    fn l1_key_format() {
        assert!(CentroidIndexL1::key("ns", "emb", 3).contains("centroids-l1-00000003"));
        assert!(CentroidIndexL1::key("ns", "emb", 3).contains("/index/emb/"));
    }

    #[test]
    fn f16_cluster_members_store_packed_half() {
        let docs = vec![
            vec_doc("a", vec![1.0, 0.0, 0.5, 0.0]),
            vec_doc("b", vec![0.0, 1.0, 0.5, 0.0]),
        ];
        let schema = json!({ "embedding": "[4]f16" });
        let index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
            &schema,
            AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("index");
        assert_eq!(index.l0.vector_element, VectorElement::F16);
        let member = index
            .clusters
            .values()
            .flat_map(|c| &c.members)
            .find(|m| m.doc_id == "a")
            .expect("doc a");
        assert!(member.vector.is_empty());
        let packed = member.vector_f16.as_ref().expect("f16 bytes");
        assert_eq!(packed.len(), 8);
        let bytes = index.clusters.values().next().unwrap().encode().unwrap();
        let back = ClusterSegment::decode(&bytes).unwrap();
        assert!(back.members[0].vector_f16.is_some());
    }

    #[test]
    fn f16_ann_finds_nearest_neighbor() {
        let mut docs = Vec::new();
        for i in 0..100 {
            let x = (i as f64) * 0.01;
            docs.push(vec_doc(
                &format!("doc-{i}"),
                vec![x, 1.0 - x, 0.5, 0.0],
            ));
        }
        docs.push(vec_doc("target", vec![1.0, 0.0, 0.5, 0.0]));
        let schema = json!({ "embedding": "[4]f16" });

        let index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
            &schema,
            AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("index");
        assert_eq!(index.l0.vector_element, VectorElement::F16);

        let query = vec![1.0, 0.0, 0.5, 0.0];
        let hits = index.query_ann(&query, 1);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "target");
        assert!(hits[0].1 > 0.99);
    }

    #[test]
    fn kmeans_plus_plus_differs_from_first_k_seeds() {
        let pairs: Vec<(String, Vec<f64>)> = (0..40)
            .map(|i| {
                let angle = (i as f64) * 0.4;
                (
                    format!("d{i}"),
                    vec![angle.cos(), angle.sin()],
                )
            })
            .collect();
        let k = 4;
        let metric = DistanceMetric::CosineDistance;
        let plus = super::kmeans_plus_plus_init(&pairs, k, metric);
        let first_k: Vec<Vec<f64>> = pairs.iter().take(k).map(|(_, v)| v.clone()).collect();
        assert_ne!(plus, first_k, "k-means++ should not equal first-k seeding");
    }
}