//! Server-side limits aligned with [turbopuffer limits](https://turbopuffer.com/docs/limits).

use crate::models::WriteRequest;

/// Namespace names must match `[A-Za-z0-9-_.]{1,128}` (turbopuffer write docs).
pub const MAX_NAMESPACE_NAME_LEN: usize = 128;

/// Max rows per `delete_by_filter` / `patch_by_filter` when `*_allow_partial` is false.
pub const DEFAULT_MAX_FILTER_BATCH_ROWS: usize = 5_000;

/// Max upsert/patch/delete rows per write request (excluding filter-based ops).
pub const DEFAULT_MAX_UPSERT_ROWS: usize = 10_000;

/// Max JSON write body size (turbopuffer document size / practical batch cap for v1).
pub const MAX_WRITE_BODY_BYTES: usize = 64 * 1024 * 1024;

fn namespace_name_char_ok(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.'
}

/// Reject path traversal and slash characters in S3 key path segments (namespace, vector field, etc.).
pub fn validate_s3_path_segment(segment: &str, label: &str) -> Result<(), String> {
    if segment.is_empty() {
        return Ok(());
    }
    if segment.contains("..") {
        return Err(format!("{label} must not contain '..'"));
    }
    if segment.contains('/') || segment.contains('\\') {
        return Err(format!("{label} must not contain path separators"));
    }
    if segment.chars().any(|c| c.is_control()) {
        return Err(format!("{label} must not contain control characters"));
    }
    Ok(())
}

/// Validate a full S3 object key before `GetObject` / `PutObject` (cold batch and index paths).
pub fn validate_s3_object_key(key: &str) -> Result<(), String> {
    if key.is_empty() {
        return Err("S3 object key must not be empty".into());
    }
    if !key.starts_with(crate::models::ROOT_PREFIX) {
        return Err(format!(
            "S3 object key must start with '{}'",
            crate::models::ROOT_PREFIX
        ));
    }
    if key.contains("..") {
        return Err("S3 object key must not contain '..'".into());
    }
    if key.contains('\\') {
        return Err("S3 object key must not contain backslashes".into());
    }
    if key.contains("//") {
        return Err("S3 object key must not contain empty path segments".into());
    }
    Ok(())
}

/// Validate a namespace path segment before S3 key use.
pub fn validate_namespace_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("namespace name must not be empty".into());
    }
    if name.len() > MAX_NAMESPACE_NAME_LEN {
        return Err(format!(
            "namespace name exceeds maximum length of {MAX_NAMESPACE_NAME_LEN} bytes"
        ));
    }
    if !name.chars().all(namespace_name_char_ok) {
        return Err(
            "namespace name must match [A-Za-z0-9-_.]{1,128} (letters, digits, hyphen, underscore, dot)"
                .into(),
        );
    }
    Ok(())
}

/// Count explicit row operations in a write request (not filter-based ops).
pub fn count_explicit_write_rows(body: &WriteRequest) -> usize {
    let mut n = body.upsert_rows.len() + body.patch_rows.len() + body.deletes.len();
    if let Some(cols) = &body.upsert_columns {
        if let Some(ids) = cols.get("id").and_then(|v| v.as_array()) {
            n += ids.len();
        }
    }
    if let Some(cols) = &body.patch_columns {
        if let Some(ids) = cols.get("id").and_then(|v| v.as_array()) {
            n += ids.len();
        }
    }
    n
}

/// Cap filter-resolved doc ids; returns `(ids, rows_remaining)`.
pub fn cap_filter_batch(
    mut ids: Vec<String>,
    allow_partial: bool,
    max_rows: usize,
) -> Result<(Vec<String>, bool), String> {
    if ids.len() <= max_rows {
        return Ok((ids, false));
    }
    if allow_partial {
        ids.truncate(max_rows);
        return Ok((ids, true));
    }
    Err(format!(
        "filter matched {} documents; maximum per request is {max_rows} \
         (set delete_by_filter_allow_partial or patch_by_filter_allow_partial to true to process in batches)",
        ids.len()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::WriteRequest;
    use serde_json::json;

    #[test]
    fn accepts_valid_namespace_names() {
        assert!(validate_namespace_name("my-ns_1.prod").is_ok());
        assert!(validate_namespace_name("A").is_ok());
    }

    #[test]
    fn rejects_invalid_namespace_names() {
        assert!(validate_namespace_name("").is_err());
        assert!(validate_namespace_name("bad/name").is_err());
        assert!(validate_namespace_name("bad space").is_err());
        let long = "a".repeat(MAX_NAMESPACE_NAME_LEN + 1);
        assert!(validate_namespace_name(&long).is_err());
    }

    #[test]
    fn rejects_path_traversal_in_s3_path_segments() {
        assert!(validate_s3_path_segment("../escape", "vector field name").is_err());
        assert!(validate_s3_path_segment("emb/foo", "vector field name").is_err());
        assert!(validate_s3_path_segment("ok_field", "vector field name").is_ok());
        assert!(validate_s3_path_segment("", "vector field name").is_ok());
    }

    #[test]
    fn rejects_traversal_in_s3_object_keys() {
        let bad = format!(
            "{}ns/index/../../other-ns/index/emb/centroids-l0.bin",
            crate::models::ROOT_PREFIX
        );
        assert!(validate_s3_object_key(&bad).is_err());
        let ok = format!("{}ns/index/emb/centroids-l0.bin", crate::models::ROOT_PREFIX);
        assert!(validate_s3_object_key(&ok).is_ok());
    }

    #[test]
    fn count_explicit_write_rows_sums_ops() {
        let body: WriteRequest = serde_json::from_value(json!({
            "upsert_rows": [{"id": "a"}, {"id": "b"}],
            "deletes": ["c"],
            "patch_rows": [{"id": "d"}],
        }))
        .unwrap();
        assert_eq!(count_explicit_write_rows(&body), 4);
    }

    #[test]
    fn cap_filter_batch_partial_truncates() {
        let ids: Vec<String> = (0..10).map(|i| format!("d{i}")).collect();
        let (capped, remaining) = cap_filter_batch(ids, true, 3).unwrap();
        assert_eq!(capped.len(), 3);
        assert!(remaining);
    }

    #[test]
    fn cap_filter_batch_rejects_without_partial() {
        let ids: Vec<String> = (0..5).map(|i| format!("d{i}")).collect();
        assert!(cap_filter_batch(ids, false, 3).is_err());
    }
}