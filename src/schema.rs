//! turbopuffer-style namespace schema: merge on write, drive indexer field selection.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

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
pub fn merge_schema(existing: &Value, patch: &Value) -> Value {
    let Some(patch_obj) = patch.as_object() else {
        return existing.clone();
    };
    let mut out = match existing.as_object() {
        Some(m) => m.clone(),
        None => Map::new(),
    };
    for (name, spec) in patch_obj {
        out.insert(name.clone(), spec.clone());
    }
    Value::Object(out)
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
        || t.contains("uuid")
        || t.contains("datetime")
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
        let merged = merge_schema(&existing, &patch);
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
    fn validate_patch_rejects_vector_field() {
        let schema = json!({"embedding": "[3]f32", "text": {"type": "string"}});
        let attrs = [("embedding".into(), json!([1.0, 0.0, 0.0]))].into();
        assert!(validate_patch_attributes(&attrs, &schema).is_err());
        let ok = [("text".into(), json!("hi"))].into();
        assert!(validate_patch_attributes(&ok, &schema).is_ok());
    }
}