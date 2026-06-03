//! turbopuffer-style namespace schema: merge on write, drive indexer field selection.

use serde_json::{Map, Value};

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
}