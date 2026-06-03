//! Vector ANN index segments (centroid / cluster layout on S3).
//!
//! Planned layout: `openpuffer/{ns}/index/centroids.bin`, `clusters-*.bin`

use crate::meta::DistanceMetric;
use serde::{Deserialize, Serialize};

/// Stub: centroid table header for SPFresh-style ANN.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CentroidIndex {
    pub num_centroids: u32,
    pub dimensions: u32,
    pub distance_metric: DistanceMetric,
}

impl CentroidIndex {
    pub fn key(namespace: &str) -> String {
        format!(
            "{}{namespace}/index/centroids.bin",
            crate::models::ROOT_PREFIX
        )
    }
}

/// Stub: one cluster segment referencing member doc ids.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterSegment {
    pub segment_id: u64,
    pub centroid_id: u32,
}

impl ClusterSegment {
    pub fn key(namespace: &str, segment_id: u64) -> String {
        format!(
            "{}{namespace}/index/clusters-{segment_id:08}.bin",
            crate::models::ROOT_PREFIX
        )
    }
}