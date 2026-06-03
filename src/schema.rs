//! turbopuffer-style namespace schema: merge on write, drive indexer field selection.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::index::vector::vector_fields_from_schema;
use crate::meta::MAX_VECTOR_FIELDS;

/// Element type for `[N]f32` vs `[N]f16` vector columns.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorElement {
    #[default]
    F32,
    F16,
}

/// Parse turbopuffer vector shorthand (`[128]f32`, `[512]f16`, …).
pub fn parse_vector_type_spec(spec: &Value) -> Option<(u32, VectorElement)> {
    let type_str = match spec {
        Value::String(s) => Some(s.as_str()),
        Value::Object(m) => m.get("type").and_then(|v| v.as_str()),
        _ => None,
    }?;
    parse_vector_type_str(type_str)
}

fn parse_vector_type_str(s: &str) -> Option<(u32, VectorElement)> {
    let s = s.trim();
    let inner = s.strip_prefix('[')?;
    let (dims_str, elem) = inner.split_once(']')?;
    let dims: u32 = dims_str.parse().ok()?;
    let elem = match elem.to_ascii_lowercase().as_str() {
        "f32" => VectorElement::F32,
        "f16" => VectorElement::F16,
        _ => return None,
    };
    Some((dims, elem))
}

/// Element type for a vector field in namespace schema (defaults to f32).
pub fn vector_element_for_field(schema: &Value, field: &str) -> VectorElement {
    schema
        .as_object()
        .and_then(|o| o.get(field))
        .and_then(parse_vector_type_spec)
        .map(|(_, e)| e)
        .unwrap_or(VectorElement::F32)
}

/// Expected dimensions from schema for a vector field (if declared).
pub fn vector_dimensions_for_field(schema: &Value, field: &str) -> Option<u32> {
    schema
        .as_object()
        .and_then(|o| o.get(field))
        .and_then(parse_vector_type_spec)
        .map(|(d, _)| d)
}

/// Merge a write-time `schema` patch into durable namespace metadata.
///
/// Field specs from the request overwrite existing entries (turbopuffer shape:
/// shorthand `"[128]f32"`, or objects with `type`, `full_text_search`, `filterable`).
/// Rejects more than [`MAX_VECTOR_FIELDS`] vector columns (turbopuffer limit).
pub fn merge_schema(existing: &Value, patch: &Value) -> Result<Value> {
    let Some(patch_obj) = patch.as_object() else {
        return Ok(existing.clone());
    };
    let mut out = match existing.as_object() {
        Some(m) => m.clone(),
        None => Map::new(),
    };
    for (name, spec) in patch_obj {
        out.insert(name.clone(), spec.clone());
    }
    let merged = Value::Object(out);
    let vector_count = vector_fields_from_schema(&merged).len();
    if vector_count > MAX_VECTOR_FIELDS {
        return Err(anyhow!(
            "namespace schema has {vector_count} vector fields; maximum is {MAX_VECTOR_FIELDS}"
        ));
    }
    Ok(merged)
}

/// Normalize a field spec to an object for introspection (type string + flags).
pub fn field_spec_object(spec: &Value) -> Option<&Map<String, Value>> {
    spec.as_object()
}

/// True when the field should participate in BM25 / FTS indexing.
pub fn field_full_text_search(spec: &Value) -> bool {
    if let Some(m) = field_spec_object(spec) {
        if m.get("full_text_search").and_then(|v| v.as_bool()) == Some(true) {
            return true;
        }
        if let Some(Value::String(t)) = m.get("type") {
            let t = t.to_ascii_lowercase();
            return t.contains("string") || t.contains("text") || t == "full_text";
        }
        return false;
    }
    if let Some(s) = spec.as_str() {
        let t = s.to_ascii_lowercase();
        return t.contains("string") || t.contains("text") || t == "full_text";
    }
    false
}

/// True when the field should be indexed for attribute filters.
pub fn field_filterable(spec: &Value) -> bool {
    if let Some(m) = field_spec_object(spec) {
        if m.get("filterable").and_then(|v| v.as_bool()) == Some(false) {
            return false;
        }
        if m.get("filterable").and_then(|v| v.as_bool()) == Some(true) {
            return true;
        }
        if field_is_vector_spec(spec) {
            return false;
        }
        if field_full_text_search(spec) {
            return true;
        }
        if let Some(Value::String(t)) = m.get("type") {
            return scalar_type_filterable(&t.to_ascii_lowercase());
        }
        return false;
    }
    if let Some(s) = spec.as_str() {
        let t = s.to_ascii_lowercase();
        if field_is_vector_type_str(&t) {
            return false;
        }
        return scalar_type_filterable(&t);
    }
    false
}

/// True when the field is a vector column (`[N]f32`, `[N]f16`, etc.).
pub fn field_is_vector_spec(spec: &Value) -> bool {
    match spec {
        Value::String(s) => field_is_vector_type_str(&s.to_ascii_lowercase()),
        Value::Object(m) => m
            .get("type")
            .and_then(|v| v.as_str())
            .map(|t| field_is_vector_type_str(&t.to_ascii_lowercase()))
            .unwrap_or(false),
        _ => false,
    }
}

fn field_is_vector_type_str(t: &str) -> bool {
    t.contains("f32") || t.contains("f16") || t.contains("vector") || t.contains("[]f")
}

/// Reject patch writes that touch vector-typed schema fields (turbopuffer returns 400).
pub fn validate_patch_attributes(
    attributes: &std::collections::HashMap<String, Value>,
    schema: &Value,
) -> Result<(), String> {
    let Some(schema_obj) = schema.as_object() else {
        return Ok(());
    };
    for key in attributes.keys() {
        if let Some(spec) = schema_obj.get(key) {
            if field_is_vector_spec(spec) {
                return Err(format!("cannot patch vector field '{key}'"));
            }
        }
    }
    Ok(())
}

fn scalar_type_filterable(t: &str) -> bool {
    t.contains("string")
        || t.contains("bool")
        || t.contains("int")
        || t.contains("uint")
        || t.contains("float")
        || t == "number"
        || t == "uuid"
        || t.contains("datetime")
}

/// Type name from shorthand or `{ "type": "..." }` object spec.
pub fn field_type_name(spec: &Value) -> Option<String> {
    match spec {
        Value::String(s) => Some(s.trim().to_ascii_lowercase()),
        Value::Object(m) => m
            .get("type")
            .and_then(|v| v.as_str())
            .map(|t| t.trim().to_ascii_lowercase()),
        _ => None,
    }
}

/// Scalar `datetime` column (turbopuffer `datetime` schema type; RFC3339 / ISO8601 strings).
pub fn field_is_datetime_scalar(spec: &Value) -> bool {
    field_type_name(spec).as_deref() == Some("datetime")
}

/// Scalar `uuid` column (turbopuffer `uuid` schema type).
pub fn field_is_uuid_scalar(spec: &Value) -> bool {
    field_type_name(spec).as_deref() == Some("uuid")
}

/// `[]uuid` column (array of UUID strings).
pub fn field_is_uuid_array(spec: &Value) -> bool {
    field_type_name(spec).as_deref() == Some("[]uuid")
}

/// Canonical UTC datetime string for filter `Gt`/`Lt` (fixed-width subseconds, `Z` suffix).
pub fn canonicalize_datetime_str(raw: &str) -> Result<String, String> {
    let dt = OffsetDateTime::parse(raw.trim(), &Rfc3339)
        .map_err(|e| format!("invalid datetime '{raw}': {e}"))?;
    dt.to_utc()
        .format(
            &time::format_description::parse(
                "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]Z",
            )
            .map_err(|e| format!("datetime format error: {e}"))?,
        )
        .map_err(|e| format!("datetime format error: {e}"))
}

/// Parse and canonicalize a UUID string (lowercase hyphenated RFC 4122).
pub fn canonicalize_uuid_str(raw: &str) -> Result<String, String> {
    Uuid::parse_str(raw.trim())
        .map(|u| u.hyphenated().to_string())
        .map_err(|e| format!("invalid uuid '{raw}': {e}"))
}

/// Validate and normalize attribute values for schema-declared `uuid` / `[]uuid` fields.
pub fn validate_and_normalize_document_attributes(
    attributes: &mut HashMap<String, Value>,
    schema: &Value,
) -> Result<(), String> {
    let Some(schema_obj) = schema.as_object() else {
        return Ok(());
    };
    for (key, value) in attributes.iter_mut() {
        let Some(spec) = schema_obj.get(key) else {
            continue;
        };
        if field_is_datetime_scalar(spec) {
            *value = normalize_datetime_scalar_value(value, key)?;
        } else if field_is_uuid_scalar(spec) {
            *value = normalize_uuid_scalar_value(value, key)?;
        } else if field_is_uuid_array(spec) {
            *value = normalize_uuid_array_value(value, key)?;
        }
    }
    Ok(())
}

fn normalize_datetime_scalar_value(value: &Value, field: &str) -> Result<Value, String> {
    let Some(s) = value.as_str() else {
        return Err(format!(
            "attribute '{field}' has type datetime; value must be an ISO8601/RFC3339 string"
        ));
    };
    Ok(Value::String(canonicalize_datetime_str(s)?))
}

fn normalize_uuid_scalar_value(value: &Value, field: &str) -> Result<Value, String> {
    let Some(s) = value.as_str() else {
        return Err(format!(
            "attribute '{field}' has type uuid; value must be a string"
        ));
    };
    Ok(Value::String(canonicalize_uuid_str(s)?))
}

fn normalize_uuid_array_value(value: &Value, field: &str) -> Result<Value, String> {
    let Some(arr) = value.as_array() else {
        return Err(format!(
            "attribute '{field}' has type []uuid; value must be an array of uuid strings"
        ));
    };
    let mut out = Vec::with_capacity(arr.len());
    for (i, item) in arr.iter().enumerate() {
        let Some(s) = item.as_str() else {
            return Err(format!(
                "attribute '{field}' []uuid element {i} must be a string"
            ));
        };
        out.push(Value::String(canonicalize_uuid_str(s)?));
    }
    Ok(Value::Array(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merge_schema_overwrites_fields() {
        let existing = json!({"text": {"type": "string"}});
        let patch = json!({
            "text": {"type": "string", "full_text_search": true},
            "tier": {"type": "string", "filterable": true}
        });
        let merged = merge_schema(&existing, &patch).unwrap();
        assert_eq!(
            merged["text"]["full_text_search"],
            json!(true)
        );
        assert_eq!(merged["tier"]["filterable"], json!(true));
    }

    #[test]
    fn full_text_and_filterable_flags() {
        let fts = json!({"type": "string", "full_text_search": true});
        assert!(field_full_text_search(&fts));
        let filt = json!({"type": "string", "filterable": true});
        assert!(field_filterable(&filt));
        let no_filt = json!({"type": "string", "filterable": false});
        assert!(!field_filterable(&no_filt));
    }

    #[test]
    fn vector_shorthand() {
        assert!(field_is_vector_spec(&json!("[128]f32")));
        assert!(!field_filterable(&json!("[128]f32")));
    }

    #[test]
    fn parse_f16_vector_type() {
        let (d, e) = parse_vector_type_spec(&json!("[512]f16")).unwrap();
        assert_eq!(d, 512);
        assert_eq!(e, VectorElement::F16);
        assert_eq!(
            vector_element_for_field(&json!({"emb": "[4]f16"}), "emb"),
            VectorElement::F16
        );
    }

    #[test]
    fn merge_schema_rejects_third_vector_field() {
        let existing = json!({
            "emb_a": "[4]f32",
            "emb_b": "[8]f32"
        });
        let patch = json!({"emb_c": "[2]f32"});
        assert!(merge_schema(&existing, &patch).is_err());
    }

    #[test]
    fn validate_patch_rejects_vector_field() {
        let schema = json!({"embedding": "[3]f32", "text": {"type": "string"}});
        let attrs = [("embedding".into(), json!([1.0, 0.0, 0.0]))].into();
        assert!(validate_patch_attributes(&attrs, &schema).is_err());
        let ok = [("text".into(), json!("hi"))].into();
        assert!(validate_patch_attributes(&ok, &schema).is_ok());
    }

    #[test]
    fn datetime_schema_type() {
        assert!(field_is_datetime_scalar(&json!("datetime")));
        assert!(field_filterable(&json!("datetime")));
        let canon = canonicalize_datetime_str("2024-06-03T09:10:54Z").unwrap();
        assert_eq!(canon, "2024-06-03T09:10:54.000000000Z");
        let with_frac = canonicalize_datetime_str("2024-01-02T03:04:05.123456789+00:00").unwrap();
        assert_eq!(with_frac, "2024-01-02T03:04:05.123456789Z");
    }

    #[test]
    fn validate_datetime_attributes_on_write() {
        let schema = json!({ "updated_at": "datetime" });
        let mut attrs = HashMap::from([("updated_at".into(), json!("2024-06-03T09:10:54Z"))]);
        validate_and_normalize_document_attributes(&mut attrs, &schema).unwrap();
        assert_eq!(
            attrs["updated_at"],
            json!("2024-06-03T09:10:54.000000000Z")
        );
    }

    #[test]
    fn validate_datetime_rejects_invalid() {
        let schema = json!({ "updated_at": "datetime" });
        let mut bad = HashMap::from([("updated_at".into(), json!("not-a-date"))]);
        assert!(validate_and_normalize_document_attributes(&mut bad, &schema).is_err());
    }

    #[test]
    fn datetime_lexicographic_order_matches_chronological() {
        let a = canonicalize_datetime_str("2024-01-01T00:00:00Z").unwrap();
        let b = canonicalize_datetime_str("2024-12-01T12:00:00Z").unwrap();
        assert!(a < b);
    }

    #[test]
    fn uuid_scalar_and_array_schema_types() {
        assert!(field_is_uuid_scalar(&json!("uuid")));
        assert!(field_is_uuid_array(&json!("[]uuid")));
        assert!(!field_is_uuid_scalar(&json!("[]uuid")));
        assert!(field_filterable(&json!("uuid")));
        assert!(!field_filterable(&json!("[]uuid")));
    }

    #[test]
    fn canonicalize_uuid_normalizes_case_and_hyphens() {
        let raw = "550E8400E29B41D4A716446655440000";
        let canon = canonicalize_uuid_str(raw).unwrap();
        assert_eq!(canon, "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn validate_uuid_attributes_on_write() {
        let schema = json!({
            "tenant_id": "uuid",
            "permissions": "[]uuid"
        });
        let mut attrs = HashMap::from([
            (
                "tenant_id".into(),
                json!("550E8400-E29B-41D4-A716-446655440001"),
            ),
            (
                "permissions".into(),
                json!(["550e8400e29b41d4a716446655440002", "550e8400-e29b-41d4-a716-446655440003"]),
            ),
        ]);
        validate_and_normalize_document_attributes(&mut attrs, &schema).unwrap();
        assert_eq!(
            attrs["tenant_id"],
            json!("550e8400-e29b-41d4-a716-446655440001")
        );
        assert_eq!(
            attrs["permissions"],
            json!([
                "550e8400-e29b-41d4-a716-446655440002",
                "550e8400-e29b-41d4-a716-446655440003"
            ])
        );
    }

    #[test]
    fn validate_uuid_rejects_invalid() {
        let schema = json!({"tenant_id": "uuid"});
        let mut bad = HashMap::from([("tenant_id".into(), json!("not-a-uuid"))]);
        assert!(validate_and_normalize_document_attributes(&mut bad, &schema).is_err());
    }
}