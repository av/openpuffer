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