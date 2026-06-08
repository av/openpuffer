//! turbopuffer-style filter DSL: `["field", "Eq", value]`, `["And", [...]]`, etc.

use crate::models::Document;
use anyhow::{anyhow, bail, Result};
use serde_json::Value;

/// Comparison operators supported in v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// Parsed filter expression tree.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterExpr {
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
    Cmp {
        field: String,
        op: CmpOp,
        value: FilterValue,
    },
}

/// Scalar filter value (string / number / bool / null).
#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    /// turbopuffer conditional writes: `{"$ref_new": "field"}` reads from the incoming row on
    /// upsert/patch; on `delete_condition` all `$ref_new` values resolve to null.
    RefNew(String),
}

impl FilterValue {
    pub fn from_json(v: &Value) -> Result<Self> {
        match v {
            Value::String(s) => Ok(FilterValue::String(s.clone())),
            Value::Bool(b) => Ok(FilterValue::Bool(*b)),
            Value::Number(n) => n
                .as_f64()
                .map(FilterValue::Number)
                .ok_or_else(|| anyhow!("filter number must be finite")),
            Value::Null => Ok(FilterValue::Null),
            Value::Object(m) => {
                if m.len() == 1 {
                    if let Some(field) = m.get("$ref_new").and_then(|f| f.as_str()) {
                        return Ok(FilterValue::RefNew(field.to_string()));
                    }
                }
                bail!(
                    "filter value object must be {{\"$ref_new\": \"field\"}} (got {})",
                    v
                );
            }
            _ => bail!(
                "filter value must be string, number, bool, null, or $ref_new (got {})",
                v
            ),
        }
    }

    pub fn from_json_array(arr: &[Value]) -> Result<Vec<Self>> {
        arr.iter().map(Self::from_json).collect()
    }
}

/// Parse turbopuffer filter JSON into an expression tree.
pub fn parse_filter(v: &Value) -> Result<FilterExpr> {
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow!("filter must be a JSON array"))?;
    if arr.is_empty() {
        bail!("filter array is empty");
    }
    let head = arr[0]
        .as_str()
        .ok_or_else(|| anyhow!("filter[0] must be a string operator or field name"))?;
    match head {
        "And" | "and" => {
            let subs = arr
                .get(1)
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow!("And filter requires a sub-array at index 1"))?;
            Ok(FilterExpr::And(
                subs.iter().map(parse_filter).collect::<Result<Vec<_>>>()?,
            ))
        }
        "Or" | "or" => {
            let subs = arr
                .get(1)
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow!("Or filter requires a sub-array at index 1"))?;
            Ok(FilterExpr::Or(
                subs.iter().map(parse_filter).collect::<Result<Vec<_>>>()?,
            ))
        }
        field => {
            if arr.len() < 3 {
                bail!("comparison filter needs [field, Op, value]");
            }
            let op_str = arr[1]
                .as_str()
                .ok_or_else(|| anyhow!("filter operator must be a string"))?;
            if op_str == "In" {
                let values = FilterValue::from_json_array(
                    arr[2]
                        .as_array()
                        .ok_or_else(|| anyhow!("In filter value must be an array"))?,
                )?;
                if values.is_empty() {
                    bail!("In filter value array cannot be empty");
                }
                return Ok(FilterExpr::Or(
                    values
                        .into_iter()
                        .map(|value| FilterExpr::Cmp {
                            field: field.to_string(),
                            op: CmpOp::Eq,
                            value,
                        })
                        .collect(),
                ));
            }
            let op = parse_cmp_op(op_str)?;
            Ok(FilterExpr::Cmp {
                field: field.to_string(),
                op,
                value: FilterValue::from_json(&arr[2])?,
            })
        }
    }
}

fn parse_cmp_op(s: &str) -> Result<CmpOp> {
    match s {
        "Eq" => Ok(CmpOp::Eq),
        "Ne" => Ok(CmpOp::Ne),
        "Gt" => Ok(CmpOp::Gt),
        "Gte" => Ok(CmpOp::Gte),
        "Lt" => Ok(CmpOp::Lt),
        "Lte" => Ok(CmpOp::Lte),
        "In" => bail!("In should be handled separately"),
        other => bail!(
            "unsupported filter operator: {other} (supported: Eq, Ne, Gt, Gte, Lt, Lte, In)"
        ),
    }
}

/// Evaluate filter against one document (WAL tail / exhaustive fallback).
pub fn eval_filter(expr: &FilterExpr, doc: &Document) -> bool {
    eval_filter_with_new(expr, doc, None, false)
}

/// Evaluate filter; when `new_doc` is set, `FilterValue::RefNew` reads from the incoming row.
/// When `ref_new_null` is true (`delete_condition`), every `$ref_new` resolves to null.
pub fn eval_filter_with_new(
    expr: &FilterExpr,
    doc: &Document,
    new_doc: Option<&Document>,
    ref_new_null: bool,
) -> bool {
    match expr {
        FilterExpr::And(subs) => subs
            .iter()
            .all(|s| eval_filter_with_new(s, doc, new_doc, ref_new_null)),
        FilterExpr::Or(subs) => subs
            .iter()
            .any(|s| eval_filter_with_new(s, doc, new_doc, ref_new_null)),
        FilterExpr::Cmp { field, op, value } => {
            eval_cmp(doc, field, *op, value, new_doc, ref_new_null)
        }
    }
}

/// turbopuffer `upsert_condition`: create if missing; otherwise evaluate on current doc.
pub fn should_apply_upsert(expr: &FilterExpr, current: Option<&Document>, new: &Document) -> bool {
    match current {
        None => true,
        Some(doc) => eval_filter_with_new(expr, doc, Some(new), false),
    }
}

/// turbopuffer `patch_condition`: skip missing ids; otherwise evaluate on current doc with patch as `$ref_new`.
pub fn should_apply_patch(expr: &FilterExpr, current: Option<&Document>, patch: &Document) -> bool {
    match current {
        None => false,
        Some(doc) => eval_filter_with_new(expr, doc, Some(patch), false),
    }
}

/// turbopuffer `delete_condition`: skip missing ids; otherwise evaluate on current doc (`$ref_new` → null).
pub fn should_apply_delete(expr: &FilterExpr, current: Option<&Document>) -> bool {
    match current {
        None => false,
        Some(doc) => eval_filter_with_new(expr, doc, None, true),
    }
}

fn eval_cmp(
    doc: &Document,
    field: &str,
    op: CmpOp,
    rhs: &FilterValue,
    new_doc: Option<&Document>,
    ref_new_null: bool,
) -> bool {
    let lhs = doc_field_value(doc, field);
    let rhs = resolve_rhs(rhs, new_doc, ref_new_null);
    match op {
        CmpOp::Eq => match (&lhs, &rhs) {
            (None, Some(FilterValue::Null)) => true,
            (None, _) => false,
            (Some(_lhs), Some(FilterValue::Null)) => false,
            (Some(lhs), Some(rhs)) => *lhs == *rhs,
            (_, None) => false,
        },
        CmpOp::Ne => match (&lhs, &rhs) {
            (None, Some(FilterValue::Null)) => false,
            (None, _) => true,
            (Some(_lhs), Some(FilterValue::Null)) => true,
            (Some(lhs), Some(rhs)) => *lhs != *rhs,
            (_, None) => false,
        },
        CmpOp::Gt => {
            let (Some(lhs), Some(rhs)) = (lhs, rhs) else {
                return false;
            };
            compare_values(&lhs, &rhs) == Some(std::cmp::Ordering::Greater)
        }
        CmpOp::Gte => {
            let (Some(lhs), Some(rhs)) = (lhs, rhs) else {
                return false;
            };
            matches!(
                compare_values(&lhs, &rhs),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            )
        }
        CmpOp::Lt => {
            let (Some(lhs), Some(rhs)) = (lhs, rhs) else {
                return false;
            };
            compare_values(&lhs, &rhs) == Some(std::cmp::Ordering::Less)
        }
        CmpOp::Lte => {
            let (Some(lhs), Some(rhs)) = (lhs, rhs) else {
                return false;
            };
            matches!(
                compare_values(&lhs, &rhs),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            )
        }
    }
}

fn resolve_rhs(
    rhs: &FilterValue,
    new_doc: Option<&Document>,
    ref_new_null: bool,
) -> Option<FilterValue> {
    match rhs {
        FilterValue::RefNew(field) => {
            if ref_new_null {
                return Some(FilterValue::Null);
            }
            let new = new_doc?;
            doc_field_value(new, field)
        }
        other => Some(other.clone()),
    }
}

/// Field value for filters: `id` uses document id; missing/null attributes → `None`.
fn doc_field_value(doc: &Document, field: &str) -> Option<FilterValue> {
    if field == "id" {
        return Some(FilterValue::String(doc.id.clone()));
    }
    let attr = doc.attributes.get(field)?;
    attr_to_filter_value(attr)
}

pub fn attr_to_filter_value(v: &Value) -> Option<FilterValue> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(FilterValue::String(s.clone())),
        Value::Bool(b) => Some(FilterValue::Bool(*b)),
        Value::Number(n) => n.as_f64().map(FilterValue::Number),
        _ => None,
    }
}

fn compare_values(lhs: &FilterValue, rhs: &FilterValue) -> Option<std::cmp::Ordering> {
    match (lhs, rhs) {
        (FilterValue::Number(a), FilterValue::Number(b)) => a.partial_cmp(b),
        (FilterValue::String(a), FilterValue::String(b)) => Some(a.cmp(b)),
        (FilterValue::Bool(a), FilterValue::Bool(b)) => Some(a.cmp(b)),
        (FilterValue::Number(a), FilterValue::Bool(b)) => {
            let bn = if *b { 1.0 } else { 0.0 };
            a.partial_cmp(&bn)
        }
        (FilterValue::Bool(a), FilterValue::Number(b)) => {
            let an = if *a { 1.0 } else { 0.0 };
            an.partial_cmp(b)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Document;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn parse_eq_filter() {
        let v = json!(["category", "Eq", "books"]);
        let f = parse_filter(&v).unwrap();
        assert_eq!(
            f,
            FilterExpr::Cmp {
                field: "category".into(),
                op: CmpOp::Eq,
                value: FilterValue::String("books".into()),
            }
        );
    }

    #[test]
    fn parse_and_or() {
        let v = json!(["And", [["a", "Eq", 1], ["Or", [["b", "Eq", true], ["c", "Ne", "x"]]]]]);
        let f = parse_filter(&v).unwrap();
        assert!(matches!(f, FilterExpr::And(_)));
    }

    #[test]
    fn parse_in_expands_to_or() {
        let v = json!(["color", "In", ["red", "blue"]]);
        let f = parse_filter(&v).unwrap();
        assert!(matches!(f, FilterExpr::Or(ref subs) if subs.len() == 2));
    }

    #[test]
    fn rejects_unsupported_op() {
        let v = json!(["x", "ContainsAny", ["a"]]);
        assert!(parse_filter(&v).is_err());
    }

    #[test]
    fn eval_eq_and_ne() {
        let doc = Document {
            id: "1".into(),
            attributes: HashMap::from([
                ("n".into(), json!(10)),
                ("s".into(), json!("hi")),
            ]),
        };
        assert!(eval_filter(
            &parse_filter(&json!(["n", "Eq", 10])).unwrap(),
            &doc
        ));
        assert!(!eval_filter(
            &parse_filter(&json!(["n", "Eq", 11])).unwrap(),
            &doc
        ));
        assert!(eval_filter(
            &parse_filter(&json!(["missing", "Ne", 0])).unwrap(),
            &doc
        ));
    }

    #[test]
    fn eval_eq_null_on_missing_field() {
        let doc = Document {
            id: "a".into(),
            attributes: HashMap::new(),
        };
        let cond = parse_filter(&json!(["tag", "Eq", null])).unwrap();
        assert!(eval_filter(&cond, &doc));
    }

    #[test]
    fn delete_only_when_status_active() {
        let active = Document {
            id: "a".into(),
            attributes: HashMap::from([("status".into(), json!("active"))]),
        };
        let inactive = Document {
            id: "b".into(),
            attributes: HashMap::from([("status".into(), json!("inactive"))]),
        };
        let cond = parse_filter(&json!(["status", "Eq", "active"])).unwrap();
        assert!(should_apply_delete(&cond, Some(&active)));
        assert!(!should_apply_delete(&cond, Some(&inactive)));
        assert!(!should_apply_delete(&cond, None));
    }

    #[test]
    fn delete_condition_ref_new_resolves_to_null() {
        let tagged = Document {
            id: "a".into(),
            attributes: HashMap::from([("tag".into(), json!("keep"))]),
        };
        let untagged = Document {
            id: "b".into(),
            attributes: HashMap::new(),
        };
        let cond = parse_filter(&json!(["tag", "Eq", {"$ref_new": "tag"}])).unwrap();
        assert!(!should_apply_delete(&cond, Some(&tagged)));
        assert!(should_apply_delete(&cond, Some(&untagged)));
    }

    #[test]
    fn patch_only_when_status_active() {
        let active = Document {
            id: "a".into(),
            attributes: HashMap::from([("status".into(), json!("active"))]),
        };
        let inactive = Document {
            id: "b".into(),
            attributes: HashMap::from([("status".into(), json!("inactive"))]),
        };
        let patch = Document {
            id: "a".into(),
            attributes: HashMap::from([("name".into(), json!("patched"))]),
        };
        let cond = parse_filter(&json!(["status", "Eq", "active"])).unwrap();
        assert!(should_apply_patch(&cond, Some(&active), &patch));
        assert!(!should_apply_patch(&cond, Some(&inactive), &patch));
        assert!(!should_apply_patch(&cond, None, &patch));
    }

    #[test]
    fn insert_if_not_exists_via_id_eq_null() {
        let existing = Document {
            id: "a".into(),
            attributes: HashMap::from([("name".into(), json!("old"))]),
        };
        let new = Document {
            id: "a".into(),
            attributes: HashMap::from([("name".into(), json!("new"))]),
        };
        let cond = parse_filter(&json!(["id", "Eq", null])).unwrap();
        assert!(!should_apply_upsert(&cond, Some(&existing), &new));
        assert!(should_apply_upsert(&cond, None, &new));
    }

    #[test]
    fn datetime_gt_lt_lexicographic() {
        let doc = Document {
            id: "1".into(),
            attributes: HashMap::from([(
                "updated_at".into(),
                json!("2024-06-01T12:00:00.000000000Z"),
            )]),
        };
        assert!(eval_filter(
            &parse_filter(&json!(["updated_at", "Gt", "2024-01-01T00:00:00.000000000Z"])).unwrap(),
            &doc
        ));
        assert!(eval_filter(
            &parse_filter(&json!(["updated_at", "Lt", "2024-12-01T12:00:00.000000000Z"])).unwrap(),
            &doc
        ));
        assert!(!eval_filter(
            &parse_filter(&json!(["updated_at", "Lt", "2024-01-01T00:00:00.000000000Z"])).unwrap(),
            &doc
        ));
    }

    #[test]
    fn newer_timestamp_upsert_condition_with_ref_new() {
        let existing = Document {
            id: "a".into(),
            attributes: HashMap::from([
                (
                    "updated_at".into(),
                    json!("2024-06-01T12:00:00.000000000Z"),
                ),
                ("title".into(), json!("old")),
            ]),
        };
        let newer = Document {
            id: "a".into(),
            attributes: HashMap::from([
                (
                    "updated_at".into(),
                    json!("2024-12-01T12:00:00.000000000Z"),
                ),
                ("title".into(), json!("new")),
            ]),
        };
        let older = Document {
            id: "a".into(),
            attributes: HashMap::from([
                (
                    "updated_at".into(),
                    json!("2024-01-01T00:00:00.000000000Z"),
                ),
                ("title".into(), json!("stale")),
            ]),
        };
        let cond = parse_filter(&json!([
            "Or",
            [
                ["updated_at", "Lt", {"$ref_new": "updated_at"}],
                ["updated_at", "Eq", null]
            ]
        ]))
        .unwrap();
        assert!(should_apply_upsert(&cond, Some(&existing), &newer));
        assert!(!should_apply_upsert(&cond, Some(&existing), &older));
        assert!(should_apply_upsert(&cond, None, &newer));
    }

    #[test]
    fn id_field_uses_document_id() {
        let doc = Document {
            id: "doc-1".into(),
            attributes: HashMap::new(),
        };
        assert!(eval_filter(
            &parse_filter(&json!(["id", "Eq", "doc-1"])).unwrap(),
            &doc
        ));
        assert!(!eval_filter(
            &parse_filter(&json!(["id", "Eq", null])).unwrap(),
            &doc
        ));
    }
}