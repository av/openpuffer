//! Background-built indexes on S3 (`openpuffer/{ns}/index/`).
//!
//! FTS inverted postings; ANN centroid/cluster segments (SPFresh-style, v1 simplified).

pub mod fts;
pub mod vector;