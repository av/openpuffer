//! Attribute filter index on S3: `openpuffer/{ns}/index/filter-{segment_id:08}.bin`
//!
//! Maps `(field, canonical_value)` → doc id sets for Eq / In / Ne and range scans.

use crate::filter::{CmpOp, FilterExpr, FilterValue};
use crate::models::Document;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Canonical key for a scalar attribute value in the inverted filter index.
pub fn value_key(v: &FilterValue) -> String {
    match v {
        FilterValue::String(s) => format!("s:{s}"),
        FilterValue::Bool(b) => format!("b:{b}"),
        FilterValue::Number(n) => format!("n:{n}"),
        FilterValue::Null => "null".to_string(),
        FilterValue::RefNew(_) => {
            unreachable!("$ref_new is only valid in upsert_condition/patch_condition, not filter index")
        }
    }
}

fn attr_to_filter_value(v: &Value) -> Option<FilterValue> {
    match v {
        Value::String(s) => Some(FilterValue::String(s.clone())),
        Value::Bool(b) => Some(FilterValue::Bool(*b)),
        Value::Number(n) => n.as_f64().map(FilterValue::Number),
        _ => None,
    }
}

/// Filter index segment: field → value_key → doc ids.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FilterSegment {
    pub segment_id: u64,
    /// All document ids present in this segment (for Ne and range scans).
    pub all_doc_ids: HashSet<String>,
    /// field → canonical value key → matching doc ids.
    pub postings: HashMap<String, HashMap<String, HashSet<String>>>,
}

impl FilterSegment {
    pub fn key(namespace: &str, segment_id: u64) -> String {
        format!(
            "{}{namespace}/index/filter-{segment_id:08}.bin",
            crate::models::ROOT_PREFIX
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context("encode filter segment")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context("decode filter segment")
    }

    /// Build from documents; indexes filterable scalar attributes.
    pub fn build(segment_id: u64, schema: &Value, docs: &[(String, Document)]) -> Self {
        let fields = filter_fields_from_schema(schema);
        let mut seg = FilterSegment {
            segment_id,
            ..Default::default()
        };
        for (id, doc) in docs {
            seg.add_document(id, doc, &fields);
        }
        seg
    }

    pub fn apply_delta(
        &mut self,
        schema: &Value,
        upserts: &[(String, Document)],
        deletes: &[String],
    ) {
        let fields = filter_fields_from_schema(schema);
        for id in deletes {
            self.remove_document(id);
        }
        for (id, doc) in upserts {
            self.remove_document(id);
            self.add_document(id, doc, &fields);
        }
    }

    fn add_document(&mut self, doc_id: &str, doc: &Document, fields: &[String]) {
        self.all_doc_ids.insert(doc_id.to_string());
        let indexed_fields: Vec<&str> = if fields.is_empty() {
            doc.attributes
                .keys()
                .map(|s| s.as_str())
                .collect()
        } else {
            fields.iter().map(|s| s.as_str()).collect()
        };
        for field in indexed_fields {
            let Some(v) = doc.attributes.get(field) else {
                continue;
            };
            let Some(fv) = attr_to_filter_value(v) else {
                continue;
            };
            let vk = value_key(&fv);
            self.postings
                .entry(field.to_string())
                .or_default()
                .entry(vk)
                .or_default()
                .insert(doc_id.to_string());
        }
    }

    fn remove_document(&mut self, doc_id: &str) {
        if !self.all_doc_ids.remove(doc_id) {
            return;
        }
        for field_map in self.postings.values_mut() {
            for set in field_map.values_mut() {
                set.remove(doc_id);
            }
            field_map.retain(|_, set| !set.is_empty());
        }
        self.postings.retain(|_, m| !m.is_empty());
    }

    /// Doc ids matching the filter expression using the inverted index.
    pub fn matching_doc_ids(&self, expr: &FilterExpr) -> HashSet<String> {
        match expr {
            FilterExpr::And(subs) => {
                let mut it = subs.iter().map(|s| self.matching_doc_ids(s));
                let Some(first) = it.next() else {
                    return self.all_doc_ids.clone();
                };
                it.fold(first, |acc, set| acc.intersection(&set).cloned().collect())
            }
            FilterExpr::Or(subs) => subs
                .iter()
                .flat_map(|s| self.matching_doc_ids(s))
                .collect(),
            FilterExpr::Cmp { field, op, value } => self.match_cmp(field, *op, value),
        }
    }

    fn match_cmp(&self, field: &str, op: CmpOp, value: &FilterValue) -> HashSet<String> {
        let field_map = self.postings.get(field);
        match op {
            CmpOp::Eq => field_map
                .and_then(|m| m.get(&value_key(value)))
                .cloned()
                .unwrap_or_default(),
            CmpOp::Ne => {
                let mut out = self.all_doc_ids.clone();
                if let Some(m) = field_map {
                    for ids in m.values() {
                        for id in ids {
                            out.remove(id);
                        }
                    }
                }
                out
            }
            CmpOp::Gt | CmpOp::Gte | CmpOp::Lt | CmpOp::Lte => {
                let mut out = HashSet::new();
                let Some(m) = field_map else {
                    return out;
                };
                for (vk, ids) in m {
                    if let Some(lhs) = parse_value_key(vk) {
                        if cmp_values(&lhs, value, op) {
                            out.extend(ids.iter().cloned());
                        }
                    }
                }
                out
            }
        }
    }
}

fn parse_value_key(vk: &str) -> Option<FilterValue> {
    if let Some(s) = vk.strip_prefix("s:") {
        return Some(FilterValue::String(s.to_string()));
    }
    if let Some(b) = vk.strip_prefix("b:") {
        return b.parse().ok().map(FilterValue::Bool);
    }
    if let Some(n) = vk.strip_prefix("n:") {
        return n.parse().ok().map(FilterValue::Number);
    }
    None
}

fn cmp_values(lhs: &FilterValue, rhs: &FilterValue, op: CmpOp) -> bool {
    let ord = match (lhs, rhs) {
        (FilterValue::Number(a), FilterValue::Number(b)) => a.partial_cmp(b),
        (FilterValue::String(a), FilterValue::String(b)) => Some(a.cmp(b)),
        (FilterValue::Bool(a), FilterValue::Bool(b)) => Some(a.cmp(b)),
        _ => None,
    };
    match (ord, op) {
        (Some(std::cmp::Ordering::Greater), CmpOp::Gt) => true,
        (Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal), CmpOp::Gte) => true,
        (Some(std::cmp::Ordering::Less), CmpOp::Lt) => true,
        (Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal), CmpOp::Lte) => true,
        _ => false,
    }
}

/// Fields to index from schema (filterable scalars).
pub fn filter_fields_from_schema(schema: &Value) -> Vec<String> {
    let Some(obj) = schema.as_object() else {
        return Vec::new();
    };
    let mut fields = Vec::new();
    for (name, spec) in obj {
        if field_is_filterable(spec) {
            fields.push(name.clone());
        }
    }
    fields
}

fn field_is_filterable(spec: &Value) -> bool {
    crate::schema::field_filterable(spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Document;
    use serde_json::json;

    fn doc(id: &str, attrs: serde_json::Map<String, Value>) -> (String, Document) {
        (
            id.to_string(),
            Document {
                id: id.to_string(),
                attributes: attrs.into_iter().collect(),
            },
        )
    }

    #[test]
    fn eq_index_returns_subset() {
        let docs = vec![
            doc(
                "a",
                serde_json::Map::from_iter([("kind".into(), json!("alpha"))]),
            ),
            doc(
                "b",
                serde_json::Map::from_iter([("kind".into(), json!("beta"))]),
            ),
            doc(
                "c",
                serde_json::Map::from_iter([("kind".into(), json!("alpha"))]),
            ),
        ];
        let seg = FilterSegment::build(1, &json!({}), &docs);
        let expr = FilterExpr::Cmp {
            field: "kind".into(),
            op: CmpOp::Eq,
            value: FilterValue::String("alpha".into()),
        };
        let ids = seg.matching_doc_ids(&expr);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains("a"));
        assert!(ids.contains("c"));
    }

    #[test]
    fn apply_delta_incremental_without_full_rebuild() {
        let docs = vec![doc(
            "a",
            serde_json::Map::from_iter([("kind".into(), json!("alpha"))]),
        )];
        let mut seg = FilterSegment::build(1, &json!({}), &docs);
        let more = vec![doc(
            "b",
            serde_json::Map::from_iter([("kind".into(), json!("beta"))]),
        )];
        seg.apply_delta(&json!({}), &more, &[]);
        assert_eq!(seg.all_doc_ids.len(), 2);
        assert_eq!(seg.segment_id, 1);
    }

    #[test]
    fn segment_roundtrip_bincode() {
        let docs = vec![doc(
            "x",
            serde_json::Map::from_iter([("n".into(), json!(42))]),
        )];
        let seg = FilterSegment::build(3, &json!({}), &docs);
        let back = FilterSegment::decode(&seg.encode().unwrap()).unwrap();
        assert_eq!(back.segment_id, 3);
        assert_eq!(back.all_doc_ids.len(), 1);
    }
}