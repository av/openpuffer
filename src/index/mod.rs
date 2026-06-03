//! Background-built indexes on S3 (`openpuffer/{ns}/index/`).
//!
//! FTS inverted postings; two-level ANN centroid/cluster segments (SPFresh-style).

pub mod filter;
pub mod fts;
pub mod fts_tokenizer;
pub mod vector;