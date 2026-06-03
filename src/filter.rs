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
            _ => bail!(
                "filter value must be string, number, or bool (got {})",
                v.to_string()
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
    match expr {
        FilterExpr::And(subs) => subs.iter().all(|s| eval_filter(s, doc)),
        FilterExpr::Or(subs) => subs.iter().any(|s| eval_filter(s, doc)),
        FilterExpr::Cmp { field, op, value } => eval_cmp(doc, field, *op, value),
    }
}

/// turbopuffer `upsert_condition`: create if missing; otherwise evaluate on current doc.
pub fn should_apply_upsert(expr: &FilterExpr, current: Option<&Document>, _new: &Document) -> bool {
    match current {
        None => true,
        Some(doc) => eval_filter(expr, doc),
    }
}

fn eval_cmp(doc: &Document, field: &str, op: CmpOp, rhs: &FilterValue) -> bool {
    let lhs = doc_field_value(doc, field);
    match op {
        CmpOp::Eq => match (&lhs, rhs) {
            (None, FilterValue::Null) => true,
            (None, _) => false,
            (Some(_lhs), FilterValue::Null) => false,
            (Some(lhs), rhs) => *lhs == *rhs,
        },
        CmpOp::Ne => match (&lhs, rhs) {
            (None, FilterValue::Null) => false,
            (None, _) => true,
            (Some(_lhs), FilterValue::Null) => true,
            (Some(lhs), rhs) => *lhs != *rhs,
        },
        CmpOp::Gt => {
            let Some(lhs) = lhs else {
                return false;
            };
            compare_values(&lhs, rhs) == Some(std::cmp::Ordering::Greater)
        }
        CmpOp::Gte => {
            let Some(lhs) = lhs else {
                return false;
            };
            matches!(
                compare_values(&lhs, rhs),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            )
        }
        CmpOp::Lt => {
            let Some(lhs) = lhs else {
                return false;
            };
            compare_values(&lhs, rhs) == Some(std::cmp::Ordering::Less)
        }
        CmpOp::Lte => {
            let Some(lhs) = lhs else {
                return false;
            };
            matches!(
                compare_values(&lhs, rhs),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            )
        }
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

fn attr_to_filter_value(v: &Value) -> Option<FilterValue> {
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