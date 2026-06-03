//! Background-built indexes on S3 (`openpuffer/{ns}/index/`).
//!
//! Iteration 3–4 will implement FTS inverted postings and ANN centroid/cluster segments.

pub mod fts;
pub mod vector;