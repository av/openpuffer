//! Full-text inverted index segments (BM25 postings on S3).
//!
//! Planned layout: `openpuffer/{ns}/index/fts-*.bin`

use serde::{Deserialize, Serialize};

/// Stub: one posting list chunk (term → doc ids + frequencies).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FtsSegment {
    pub segment_id: u64,
    pub term_count: u64,
}

impl FtsSegment {
    pub fn key(namespace: &str, segment_id: u64) -> String {
        format!(
            "{}{namespace}/index/fts-{segment_id:08}.bin",
            crate::models::ROOT_PREFIX
        )
    }
}