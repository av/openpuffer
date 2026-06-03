//! Background-built indexes on S3 (`openpuffer/{ns}/index/`).
//!
//! FTS inverted postings (iter 3); ANN centroid/cluster segments (iter 4).

pub mod fts;
pub mod vector;