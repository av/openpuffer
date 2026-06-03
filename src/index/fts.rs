//! Full-text inverted index segments (BM25 postings on S3).
//!
//! Layout: `openpuffer/{ns}/index/fts-{segment_id:08}.bin` (bincode).

use crate::models::Document;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// BM25 hyperparameters (Robertson–Walker IDF).
const K1: f64 = 1.2;
const B: f64 = 0.75;

/// One posting: document id and within-document term frequency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Posting {
    pub doc_id: String,
    pub term_freq: u32,
}

/// Inverted index segment: term → posting list, plus collection stats for BM25.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FtsSegment {
    pub segment_id: u64,
    /// Field indexed in this segment (empty = all string attributes).
    pub field: String,
    pub num_docs: u32,
    pub total_doc_len: u64,
    pub doc_lengths: HashMap<String, u32>,
    pub postings: HashMap<String, Vec<Posting>>,
}

impl FtsSegment {
    pub fn key(namespace: &str, segment_id: u64) -> String {
        format!(
            "{}{namespace}/index/fts-{segment_id:08}.bin",
            crate::models::ROOT_PREFIX
        )
    }

    pub fn avg_doc_len(&self) -> f64 {
        if self.num_docs == 0 {
            return 1.0;
        }
        self.total_doc_len as f64 / self.num_docs as f64
    }

    /// Build a fresh segment from documents (field = "" indexes all string attrs).
    pub fn build(segment_id: u64, field: &str, docs: &[(String, Document)]) -> Self {
        let mut seg = FtsSegment {
            segment_id,
            field: field.to_string(),
            ..Default::default()
        };
        for (id, doc) in docs {
            seg.add_document(id, doc);
        }
        seg
    }

    /// Merge WAL upserts/deletes into an existing segment.
    pub fn apply_delta(
        &mut self,
        upserts: &[(String, Document)],
        deletes: &[String],
    ) {
        for id in deletes {
            self.remove_document(id);
        }
        for (id, doc) in upserts {
            self.remove_document(id);
            self.add_document(id, doc);
        }
    }

    fn add_document(&mut self, doc_id: &str, doc: &Document) {
        let text = extract_index_text(doc, &self.field);
        let tokens = tokenize(&text);
        if tokens.is_empty() {
            return;
        }

        let len = tokens.len() as u32;
        self.doc_lengths.insert(doc_id.to_string(), len);
        self.total_doc_len += len as u64;
        self.num_docs += 1;

        let mut term_freq: HashMap<String, u32> = HashMap::new();
        for t in tokens {
            *term_freq.entry(t).or_default() += 1;
        }
        for (term, tf) in term_freq {
            self.postings
                .entry(term)
                .or_default()
                .push(Posting {
                    doc_id: doc_id.to_string(),
                    term_freq: tf,
                });
        }
    }

    fn remove_document(&mut self, doc_id: &str) {
        let Some(len) = self.doc_lengths.remove(doc_id) else {
            return;
        };
        self.total_doc_len = self.total_doc_len.saturating_sub(len as u64);
        self.num_docs = self.num_docs.saturating_sub(1);

        for list in self.postings.values_mut() {
            list.retain(|p| p.doc_id != doc_id);
        }
        self.postings.retain(|_, list| !list.is_empty());
    }

    /// BM25 scores for candidate doc ids from posting lists only.
    pub fn query_bm25(&self, query: &str, top_k: usize) -> Vec<(String, f64)> {
        let terms: Vec<String> = tokenize(query);
        if terms.is_empty() || self.num_docs == 0 {
            return Vec::new();
        }

        let n = self.num_docs as f64;
        let avgdl = self.avg_doc_len();
        let mut scores: HashMap<String, f64> = HashMap::new();

        for term in terms {
            let Some(list) = self.postings.get(&term) else {
                continue;
            };
            let df = list.len() as f64;
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();

            for posting in list {
                let dl = *self.doc_lengths.get(&posting.doc_id).unwrap_or(&1) as f64;
                let tf = posting.term_freq as f64;
                let num = tf * (K1 + 1.0);
                let den = tf + K1 * (1.0 - B + B * dl / avgdl);
                let score = idf * num / den;
                *scores.entry(posting.doc_id.clone()).or_default() += score;
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

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context("encode FtsSegment")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context("decode FtsSegment")
    }
}

/// Tokenize: lowercase alphanumeric tokens.
pub fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut cur = String::new();
    for ch in text.to_lowercase().chars() {
        if ch.is_alphanumeric() {
            cur.push(ch);
        } else if !cur.is_empty() {
            tokens.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        tokens.push(cur);
    }
    tokens
}

/// Extract searchable text for a document and field selector.
pub fn extract_index_text(doc: &Document, field: &str) -> String {
    if !field.is_empty() {
        return doc
            .attributes
            .get(field)
            .map(value_to_text)
            .unwrap_or_default();
    }
    let mut parts = Vec::new();
    for v in doc.attributes.values() {
        if let Value::String(s) = v {
            parts.push(s.as_str());
        }
    }
    parts.join(" ")
}

fn value_to_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        _ => v.to_string(),
    }
}

/// Fields to index from schema hints (string / full_text types), or all string attrs.
pub fn index_fields_from_schema(schema: &Value) -> Vec<String> {
    let Some(obj) = schema.as_object() else {
        return Vec::new();
    };
    let mut fields = Vec::new();
    for (name, spec) in obj {
        if field_is_indexable(spec) {
            fields.push(name.clone());
        }
    }
    fields
}

fn field_is_indexable(spec: &Value) -> bool {
    match spec {
        Value::String(s) => {
            let t = s.to_lowercase();
            t.contains("string") || t.contains("text") || t == "full_text"
        }
        Value::Object(m) => {
            if let Some(Value::String(t)) = m.get("type") {
                let t = t.to_lowercase();
                return t.contains("string") || t.contains("text") || t == "full_text";
            }
            false
        }
        _ => false,
    }
}

/// Per-field segments for schema-driven indexing; empty schema → one segment (all strings).
pub fn build_segments_for_docs(
    segment_id: u64,
    schema: &Value,
    docs: &[(String, Document)],
) -> Vec<FtsSegment> {
    let fields = index_fields_from_schema(schema);
    if fields.is_empty() {
        return vec![FtsSegment::build(segment_id, "", docs)];
    }
    fields
        .into_iter()
        .map(|f| FtsSegment::build(segment_id, &f, docs))
        .collect()
}

/// BM25 over a single in-memory document (unindexed WAL tail).
pub fn bm25_doc_score(document: &str, query: &str, avgdl: f64, num_docs: u32) -> f64 {
    let terms: Vec<String> = tokenize(query);
    if terms.is_empty() {
        return 0.0;
    }
    let doc_tokens = tokenize(document);
    if doc_tokens.is_empty() {
        return 0.0;
    }
    let dl = doc_tokens.len() as f64;
    let avgdl = avgdl.max(1.0);
    let n = num_docs.max(1) as f64;
    let mut score = 0.0;
    for term in terms {
        let tf = doc_tokens.iter().filter(|t| *t == &term).count() as f64;
        if tf == 0.0 {
            continue;
        }
        let df = 1.0;
        let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
        let num = tf * (K1 + 1.0);
        let den = tf + K1 * (1.0 - B + B * dl / avgdl);
        score += idf * num / den;
    }
    score
}

/// Doc ids touched by WAL entries in a range (for tail exhaustive scan).
pub fn wal_touched_doc_ids(entries: &[crate::wal::WalEntry]) -> HashSet<String> {
    let mut ids = HashSet::new();
    for entry in entries {
        for id in &entry.deletes {
            ids.insert(id.clone());
        }
        for u in &entry.upserts {
            ids.insert(u.id.clone());
        }
    }
    ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Document;
    use serde_json::json;

    fn doc(id: &str, text: &str) -> (String, Document) {
        (
            id.to_string(),
            Document {
                id: id.to_string(),
                attributes: [("text".into(), json!(text))].into(),
            },
        )
    }

    #[test]
    fn build_and_query_returns_top_doc() {
        let docs = vec![
            doc("a", "rust fast systems programming"),
            doc("b", "python slow scripting language"),
            doc("c", "rust ownership borrow checker"),
        ];
        let seg = FtsSegment::build(1, "text", &docs);
        let hits = seg.query_bm25("rust programming", 2);
        assert!(!hits.is_empty());
        assert_eq!(hits[0].0, "a");
        assert!(hits[0].1 > 0.0);
    }

    #[test]
    fn segment_roundtrip_bincode() {
        let docs = vec![doc("x", "hello world")];
        let seg = FtsSegment::build(7, "text", &docs);
        let bytes = seg.encode().unwrap();
        let back = FtsSegment::decode(&bytes).unwrap();
        assert_eq!(back.segment_id, 7);
        assert_eq!(back.num_docs, 1);
        let hits = back.query_bm25("hello", 1);
        assert_eq!(hits[0].0, "x");
    }

    #[test]
    fn apply_delta_removes_deleted_doc() {
        let docs = vec![doc("a", "foo bar"), doc("b", "baz qux")];
        let mut seg = FtsSegment::build(1, "text", &docs);
        seg.apply_delta(&[], &["a".into()]);
        let hits = seg.query_bm25("foo", 5);
        assert!(hits.is_empty());
        let hits_b = seg.query_bm25("baz", 5);
        assert_eq!(hits_b[0].0, "b");
    }

    #[test]
    fn tokenize_splits_punctuation() {
        assert_eq!(
            tokenize("Hello, Rust-world!"),
            vec!["hello", "rust", "world"]
        );
    }
}