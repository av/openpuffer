//! turbopuffer vector wire format: JSON float arrays or base64 little-endian f32.

use anyhow::{bail, Result};
use base64::Engine;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

use crate::index::vector::vector_fields_from_schema;
use crate::meta::NamespaceMeta;
use crate::models::Document;
use crate::schema::field_is_vector_spec;

/// Query response / upsert wire encoding for vector columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorEncoding {
    #[default]
    Float,
    Base64,
}

impl VectorEncoding {
    pub fn parse(s: Option<&str>) -> Result<Self> {
        match s.map(|x| x.to_ascii_lowercase()).as_deref() {
            None | Some("float") => Ok(Self::Float),
            Some("base64") => Ok(Self::Base64),
            Some(other) => bail!("unknown vector_encoding: {other} (use float or base64)"),
        }
    }
}

/// Which vector attribute names to return on query (`include_vectors`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncludeVectors {
    /// Do not add vectors beyond what `include_attributes` already selects.
    Unspecified,
    /// Omit all vector fields from the response.
    Exclude,
    /// Include every vector field known from schema or document shape.
    All,
    /// Include only these vector attribute names.
    Fields(Vec<String>),
}

impl IncludeVectors {
    pub fn parse(v: Option<&Value>) -> Result<Self> {
        let Some(v) = v else {
            return Ok(Self::Unspecified);
        };
        if v.is_null() {
            return Ok(Self::Unspecified);
        }
        if let Some(b) = v.as_bool() {
            return Ok(if b { Self::All } else { Self::Exclude });
        }
        let arr = v
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("include_vectors must be true, false, or an array of field names"))?;
        let fields: Vec<String> = arr
            .iter()
            .map(|x| {
                x.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| anyhow::anyhow!("include_vectors field names must be strings"))
            })
            .collect::<Result<_>>()?;
        if fields.is_empty() {
            bail!("include_vectors field list must not be empty");
        }
        Ok(Self::Fields(fields))
    }
}

/// Decode a vector attribute from JSON (float array or turbopuffer base64 f32 LE).
pub fn decode_vector_value(v: &Value) -> Result<Vec<f64>> {
    if let Some(s) = v.as_str() {
        return decode_base64_f32_le(s);
    }
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("expected vector array or base64 string"))?;
    arr.iter()
        .map(|x| {
            x.as_f64()
                .or_else(|| x.as_i64().map(|i| i as f64))
                .ok_or_else(|| anyhow::anyhow!("vector element must be number"))
        })
        .collect()
}

fn decode_base64_f32_le(s: &str) -> Result<Vec<f64>> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(s.trim())
        .map_err(|e| anyhow::anyhow!("invalid base64 vector: {e}"))?;
    if bytes.len() % 4 != 0 {
        bail!("base64 vector byte length must be a multiple of 4");
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]) as f64)
        .collect())
}

/// Encode a vector for API responses per `vector_encoding`.
pub fn encode_vector(values: &[f64], encoding: VectorEncoding) -> Value {
    match encoding {
        VectorEncoding::Float => Value::Array(values.iter().map(|&x| json!(x)).collect()),
        VectorEncoding::Base64 => {
            let mut bytes = Vec::with_capacity(values.len() * 4);
            for &x in values {
                bytes.extend_from_slice(&(x as f32).to_le_bytes());
            }
            Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
        }
    }
}

/// True when the value is a vector (numeric array or base64 f32 blob).
pub fn is_vector_value(v: &Value) -> bool {
    decode_vector_value(v).is_ok()
}

/// Vector field names from namespace schema, else infer from document attributes.
pub fn vector_field_names(meta: &NamespaceMeta, doc: Option<&Document>) -> Vec<String> {
    let from_schema = vector_fields_from_schema(&meta.schema);
    if !from_schema.is_empty() {
        return from_schema;
    }
    if let Some(doc) = doc {
        return doc
            .attributes
            .iter()
            .filter(|(_, v)| is_vector_value(v))
            .map(|(k, _)| k.clone())
            .collect();
    }
    Vec::new()
}

fn field_is_vector_in_schema(meta: &NamespaceMeta, name: &str) -> bool {
    meta.schema
        .as_object()
        .and_then(|o| o.get(name))
        .map(field_is_vector_spec)
        .unwrap_or(false)
}

/// Normalize upsert/patch attributes: decode base64 vectors to float arrays for storage.
pub fn normalize_document_vectors(attrs: &mut HashMap<String, Value>, meta: &NamespaceMeta) -> Result<()> {
    let keys: Vec<String> = attrs.keys().cloned().collect();
    for key in keys {
        let is_vec = field_is_vector_in_schema(meta, &key)
            || attrs
                .get(&key)
                .map(is_vector_value)
                .unwrap_or(false);
        if !is_vec {
            continue;
        }
        let Some(raw) = attrs.remove(&key) else {
            continue;
        };
        let decoded = decode_vector_value(&raw)?;
        attrs.insert(key, Value::Array(decoded.iter().map(|&x| json!(x)).collect()));
    }
    Ok(())
}

/// Build projected attributes for a query row.
pub fn project_row_attributes(
    doc: &Document,
    meta: &NamespaceMeta,
    include_attrs: bool,
    include_attr_names: Option<&HashSet<String>>,
    include_vectors: &IncludeVectors,
    vector_encoding: VectorEncoding,
) -> Option<HashMap<String, Value>> {
    if !include_attrs && !matches!(include_vectors, IncludeVectors::All | IncludeVectors::Fields(_)) {
        return None;
    }

    let vector_names: Vec<String> = match include_vectors {
        IncludeVectors::Unspecified | IncludeVectors::Exclude => Vec::new(),
        IncludeVectors::All => vector_field_names(meta, Some(doc)),
        IncludeVectors::Fields(fields) => fields.clone(),
    };

    let mut out = HashMap::new();

    if include_attrs {
        match include_attr_names {
            None => {
                for (k, v) in &doc.attributes {
                    if matches!(include_vectors, IncludeVectors::Exclude)
                        && (field_is_vector_in_schema(meta, k) || is_vector_value(v))
                    {
                        continue;
                    }
                    if matches!(include_vectors, IncludeVectors::Unspecified)
                        && (field_is_vector_in_schema(meta, k) || is_vector_value(v))
                    {
                        continue;
                    }
                    out.insert(k.clone(), v.clone());
                }
            }
            Some(names) => {
                for k in names {
                    if let Some(v) = doc.attributes.get(k) {
                        if matches!(include_vectors, IncludeVectors::Exclude)
                            && (field_is_vector_in_schema(meta, k) || is_vector_value(v))
                        {
                            continue;
                        }
                        out.insert(k.clone(), v.clone());
                    }
                }
            }
        }
    }

    for field in vector_names {
        let Some(raw) = doc.attributes.get(&field) else {
            continue;
        };
        if let Ok(vec) = decode_vector_value(raw) {
            out.insert(field, encode_vector(&vec, vector_encoding));
        }
    }

    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn base64_roundtrip_matches_float_array() {
        let floats = [1.0f32, 2.0, 3.5];
        let mut bytes = Vec::new();
        for f in floats {
            bytes.extend_from_slice(&f.to_le_bytes());
        }
        let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
        let decoded = decode_vector_value(&json!(b64)).unwrap();
        assert_eq!(decoded.len(), 3);
        assert!((decoded[0] - 1.0).abs() < 1e-6);
        assert!((decoded[2] - 3.5).abs() < 1e-5);
    }

    #[test]
    fn encode_base64_matches_turbopuffer_layout() {
        let enc = encode_vector(&[1.0, 0.0], VectorEncoding::Base64);
        let s = enc.as_str().unwrap();
        let back = decode_base64_f32_le(s).unwrap();
        assert_eq!(back.len(), 2);
        assert!((back[0] - 1.0).abs() < 1e-6);
        assert!((back[1]).abs() < 1e-6);
    }

    #[test]
    fn include_vectors_true_adds_vector_when_attrs_disabled() {
        let meta = NamespaceMeta {
            schema: json!({ "embedding": "[2]f32" }),
            ..Default::default()
        };
        let doc = Document {
            id: "a".into(),
            attributes: [
                ("text".into(), json!("hi")),
                ("embedding".into(), json!([1.0, 0.0])),
            ]
            .into_iter()
            .collect(),
        };
        let attrs = project_row_attributes(
            &doc,
            &meta,
            false,
            None,
            &IncludeVectors::All,
            VectorEncoding::Float,
        )
        .unwrap();
        assert_eq!(attrs.get("text"), None);
        assert_eq!(attrs["embedding"], json!([1.0, 0.0]));
    }
}