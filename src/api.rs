use crate::config::AppConfig;
use crate::export::MAX_EXPORT_LIMIT;
use crate::models::{
    validate_doc_id, Document, ExportRequest, ExportResponse, HealthResponse, NamespaceSummary,
    NamespacesResponse, QueryRequest, WriteRequest,
};
use crate::schema::{merge_schema, validate_patch_attributes};
use crate::filter::parse_filter;
use crate::storage::s3_error_hint;
use crate::search;
use crate::storage::Storage;
use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};

/// Max JSON write body (large `upsert_columns` batches, e.g. 2k × 128-dim vectors).
const MAX_WRITE_BODY_BYTES: usize = 64 * 1024 * 1024;
use crate::models::QueryPerformance;
use std::sync::Arc;
use tracing::error;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<Storage>,
    pub config: AppConfig,
}

pub fn router(state: AppState) -> Router {
    let r = Router::new()
        .route("/health", get(health))
        .route("/v1/namespaces", get(list_namespaces))
        .route("/v1/namespaces/{name}", get(get_namespace_metadata))
        .route(
            "/v1/namespaces/{name}/export",
            get(export_namespace_get).post(export_namespace_post),
        )
        .route("/v1/namespaces/{name}/warm", post(warm_namespace_handler))
        .route("/v2/namespaces/{name}", post(write_namespace))
        .route("/v2/namespaces/{name}/query", post(query_namespace))
        .route("/v2/namespaces/{name}", delete(delete_namespace));

    #[cfg(feature = "integration")]
    let r = r
        .route("/v1/debug/cache-stats", get(cache_stats_debug))
        .route("/v1/debug/cache-stats/reset", post(cache_stats_reset_debug));

    r.layer(DefaultBodyLimit::max(MAX_WRITE_BODY_BYTES))
        .with_state(state)
}

#[derive(Debug, Default, serde::Deserialize)]
struct HealthQuery {
    #[serde(default)]
    deep: Option<u8>,
}

async fn health(
    State(state): State<AppState>,
    Query(params): Query<HealthQuery>,
) -> impl IntoResponse {
    let deep = params.deep == Some(1);
    if !deep {
        return (
            StatusCode::OK,
            Json(HealthResponse {
                status: "ok",
                s3: None,
                deep: None,
            }),
        )
            .into_response();
    }

    match state.storage.deep_health_probe().await {
        Ok(()) => (
            StatusCode::OK,
            Json(HealthResponse {
                status: "ok",
                s3: Some("ok"),
                deep: Some(true),
            }),
        )
            .into_response(),
        Err(e) => {
            error!("deep health S3 probe failed: {e:#}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(HealthResponse {
                    status: "degraded",
                    s3: Some("unavailable"),
                    deep: Some(true),
                }),
            )
                .into_response()
        }
    }
}

async fn list_namespaces(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.list_namespaces().await {
        Ok(names) => {
            let mut namespaces = Vec::with_capacity(names.len());
            for id in names {
                let summary = match state.storage.namespace_summary(&id).await {
                    Ok(summary) => summary,
                    Err(_) => NamespaceSummary {
                        id,
                        index_cursor: None,
                        wal_commit_seq: None,
                        unindexed_bytes: None,
                        index_status: None,
                    },
                };
                namespaces.push(summary);
            }
            (StatusCode::OK, Json(NamespacesResponse { namespaces })).into_response()
        }
        Err(e) => {
            error!("list namespaces: {e:#}");
            storage_error_response(e)
        }
    }
}

#[derive(Debug, Default, serde::Deserialize)]
struct ExportQueryParams {
    #[serde(default)]
    last_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    format: Option<String>,
}

async fn export_namespace_get(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<ExportQueryParams>,
) -> impl IntoResponse {
    export_namespace_impl(
        &state,
        &name,
        params.last_id.as_deref(),
        params.limit,
        params.format.as_deref(),
    )
    .await
}

async fn export_namespace_post(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Option<Json<ExportRequest>>,
) -> impl IntoResponse {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    export_namespace_impl(
        &state,
        &name,
        req.last_id.as_deref(),
        req.limit,
        req.format.as_deref(),
    )
    .await
}

async fn export_namespace_impl(
    state: &AppState,
    name: &str,
    last_id: Option<&str>,
    limit: Option<usize>,
    format: Option<&str>,
) -> axum::response::Response {
    if let Some(id) = last_id {
        if let Err(msg) = validate_doc_id(id) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": msg})),
            )
                .into_response();
        }
    }
    let limit = limit.map(|n| n.min(MAX_EXPORT_LIMIT));
    match state
        .storage
        .export_namespace_page(name, last_id, limit)
        .await
    {
        Ok(page) => export_page_response(page, format),
        Err(e) => {
            error!("export namespace {name}: {e:#}");
            if e.to_string().contains("namespace not found") {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": "namespace not found"})),
                )
                    .into_response();
            }
            storage_error_response(e)
        }
    }
}

fn export_page_response(
    page: crate::export::ExportPage,
    format: Option<&str>,
) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    if let Ok(v) = HeaderValue::from_str(&page.wal_commit_seq.to_string()) {
        headers.insert("X-Openpuffer-Wal-Commit-Seq", v);
    }
    if let Some(ref next) = page.next_last_id {
        if let Ok(v) = HeaderValue::from_str(next) {
            headers.insert("X-Openpuffer-Export-Next-Last-Id", v);
        }
    }

    if format.map(|f| f.eq_ignore_ascii_case("ndjson")).unwrap_or(false) {
        let mut body = String::new();
        for (i, row) in page.rows.iter().enumerate() {
            if i > 0 {
                body.push('\n');
            }
            match serde_json::to_string(row) {
                Ok(line) => body.push_str(&line),
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": e.to_string()})),
                    )
                        .into_response();
                }
            }
        }
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-ndjson"),
        );
        return (StatusCode::OK, headers, body).into_response();
    }

    let resp = ExportResponse {
        wal_commit_seq: page.wal_commit_seq,
        rows: page.rows,
        next_last_id: page.next_last_id,
    };
    (StatusCode::OK, headers, Json(resp)).into_response()
}

async fn warm_namespace_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.storage.warm_namespace(&name).await {
        Ok(stats) => (StatusCode::OK, Json(stats)).into_response(),
        Err(e) => {
            error!("warm namespace {name}: {e:#}");
            if e.to_string().contains("namespace not found") {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": "namespace not found"})),
                )
                    .into_response();
            }
            storage_error_response(e)
        }
    }
}

#[cfg(feature = "integration")]
async fn cache_stats_debug(State(state): State<AppState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "s3_get_count": state.storage.segment_cache().s3_get_count(),
        })),
    )
}

#[cfg(feature = "integration")]
async fn cache_stats_reset_debug(State(state): State<AppState>) -> impl IntoResponse {
    state.storage.segment_cache().reset_s3_get_count();
    (
        StatusCode::OK,
        Json(serde_json::json!({"s3_get_count": 0})),
    )
}

async fn get_namespace_metadata(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.storage.namespace_metadata(&name).await {
        Ok(meta) => (StatusCode::OK, Json(meta)).into_response(),
        Err(e) => {
            error!("namespace metadata {name}: {e:#}");
            if e.to_string().contains("namespace not found") {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": "namespace not found"})),
                )
                    .into_response();
            }
            storage_error_response(e)
        }
    }
}

fn write_has_row_ops(body: &WriteRequest) -> bool {
    !body.upsert_rows.is_empty()
        || body.upsert_columns.is_some()
        || !body.patch_rows.is_empty()
        || body.patch_columns.is_some()
        || !body.deletes.is_empty()
        || body.schema.is_some()
        || body
            .delete_by_filter
            .as_ref()
            .is_some_and(|v| !v.is_null())
}

async fn write_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<WriteRequest>,
) -> impl IntoResponse {
    if let Some(source) = body.copy_from_namespace.as_deref() {
        if source.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "copy_from_namespace must not be empty"})),
            )
                .into_response();
        }
        if write_has_row_ops(&body) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "copy_from_namespace cannot be combined with other write operations"
                })),
            )
                .into_response();
        }
        match state.storage.copy_from_namespace(&name, source).await {
            Ok(()) => {
                return (
                    StatusCode::OK,
                    Json(crate::models::WriteResponse::from_stats(
                        name,
                        crate::models::WriteStats::default(),
                    )),
                )
                    .into_response();
            }
            Err(e) => {
                error!("copy namespace {name} from {source}: {e:#}");
                return copy_namespace_error_response(e);
            }
        }
    }

    let mut upserts = Vec::new();
    for row in body.upsert_rows {
        if let Err(msg) = validate_doc_id(&row.id) {
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": msg}))).into_response();
        }
        upserts.push(Document {
            id: row.id,
            attributes: row.attributes,
        });
    }
    for id in &body.deletes {
        if let Err(msg) = validate_doc_id(id) {
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": msg}))).into_response();
        }
    }

    if let Some(cols) = body.upsert_columns {
        match apply_column_batch(&mut upserts, cols, false) {
            Ok(()) => {}
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": err})),
                )
                    .into_response();
            }
        }
    }

    let mut patches = Vec::new();
    for row in body.patch_rows {
        if let Err(msg) = validate_doc_id(&row.id) {
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": msg}))).into_response();
        }
        patches.push(Document {
            id: row.id,
            attributes: row.attributes,
        });
    }
    if let Some(cols) = body.patch_columns {
        match apply_column_batch(&mut patches, cols, true) {
            Ok(()) => {}
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": err})),
                )
                    .into_response();
            }
        }
    }

    let effective_schema =
        effective_write_schema(&state, &name, body.schema.as_ref(), !patches.is_empty()).await;
    for patch in &patches {
        if let Err(msg) = validate_patch_attributes(&patch.attributes, &effective_schema) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": msg})),
            )
                .into_response();
        }
    }

    let upsert_condition = match body
        .upsert_condition
        .as_ref()
        .filter(|v| !v.is_null())
    {
        None => None,
        Some(v) => match parse_filter(v) {
            Ok(_) => body.upsert_condition.clone(),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("{e:#}")})),
                )
                    .into_response();
            }
        },
    };

    match state
        .storage
        .write_documents(
            &name,
            upserts,
            patches,
            body.deletes,
            body.schema,
            body.delete_by_filter,
            upsert_condition,
        )
        .await
    {
        Ok(stats) => (
            StatusCode::OK,
            Json(crate::models::WriteResponse::from_stats(name, stats)),
        )
            .into_response(),
        Err(e) => {
            error!("write namespace {name}: {e:#}");
            storage_error_response(e)
        }
    }
}

async fn effective_write_schema(
    state: &AppState,
    namespace: &str,
    request_schema: Option<&serde_json::Value>,
    needs_existing_schema: bool,
) -> serde_json::Value {
    if request_schema.is_none() && !needs_existing_schema {
        return serde_json::json!({});
    }
    let base = match crate::namespace::fetch_meta(
        state.storage.client(),
        state.storage.bucket(),
        namespace,
    )
    .await
    {
        Ok(Some((meta, _))) => meta.schema,
        _ => serde_json::json!({}),
    };
    match request_schema {
        Some(patch) => merge_schema(&base, patch),
        None => base,
    }
}

fn apply_column_batch(
    rows: &mut Vec<Document>,
    cols: serde_json::Value,
    patch_mode: bool,
) -> Result<(), String> {
    let col_label = if patch_mode {
        "patch_columns"
    } else {
        "upsert_columns"
    };
    let obj = cols
        .as_object()
        .ok_or_else(|| format!("{col_label} must be an object"))?;
    let id_col = obj
        .get("id")
        .and_then(|v| v.as_array())
        .ok_or_else(|| format!("{col_label} requires id column"))?;
    let n = id_col.len();
    for (key, values) in obj {
        if key == "id" {
            continue;
        }
        let arr = values
            .as_array()
            .ok_or_else(|| format!("column {key} must be an array"))?;
        if arr.len() != n {
            return Err("column length mismatch".into());
        }
    }
    for i in 0..n {
        let id = id_col[i]
            .as_str()
            .ok_or_else(|| "id values must be strings".to_string())?
            .to_string();
        if let Err(msg) = validate_doc_id(&id) {
            return Err(msg);
        }
        let mut attrs = std::collections::HashMap::new();
        for (key, values) in obj {
            if key == "id" {
                continue;
            }
            if let Some(v) = values.as_array().and_then(|a| a.get(i)) {
                attrs.insert(key.clone(), v.clone());
            }
        }
        rows.push(Document { id, attributes: attrs });
    }
    Ok(())
}

async fn query_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<QueryRequest>,
) -> impl IntoResponse {
    match state.storage.load_namespace(&name).await {
        Ok(loaded) => {
            let ctx = search::QueryContext {
                docs: &loaded.docs,
                meta: &loaded.meta,
                fts: loaded.fts.as_ref(),
                vector: loaded.vector.as_ref(),
                filter_index: loaded.filter_index.as_ref(),
                tail_doc_ids: &loaded.tail_doc_ids,
                consistency: search::QueryConsistency::default(),
                storage_roundtrips: loaded.storage_roundtrips,
            };
            match search::execute_query(&ctx, &body) {
            Ok(resp) => {
                let headers = query_performance_headers(resp.performance.as_ref());
                (StatusCode::OK, headers, Json(resp)).into_response()
            }
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response(),
            }
        }
        Err(e) => {
            error!("query load {name}: {e:#}");
            storage_error_response(e)
        }
    }
}

async fn delete_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.storage.delete_namespace(&name).await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "deleted", "namespace": name})),
        )
            .into_response(),
        Err(e) => {
            error!("delete namespace {name}: {e:#}");
            if e.to_string().contains("namespace not found") {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": "namespace not found"})),
                )
                    .into_response();
            }
            storage_error_response(e)
        }
    }
}

/// `X-Openpuffer-Candidates` and fraction header for indexed-query regression checks.
fn query_performance_headers(perf: Option<&QueryPerformance>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let Some(perf) = perf else {
        return headers;
    };
    if let Ok(v) = HeaderValue::from_str(&perf.candidates.to_string()) {
        headers.insert("X-Openpuffer-Candidates", v);
    }
    let fraction = format!("{}/{}", perf.candidates, perf.approx_namespace_size);
    if let Ok(v) = HeaderValue::from_str(&fraction) {
        headers.insert("X-Openpuffer-Candidates-Fraction", v);
    }
    if let Some(rt) = perf.storage_roundtrips {
        if let Ok(v) = HeaderValue::from_str(&rt.to_string()) {
            headers.insert("X-Openpuffer-Storage-Roundtrips", v);
        }
    }
    headers
}

fn copy_namespace_error_response(e: impl Into<anyhow::Error>) -> axum::response::Response {
    let err: anyhow::Error = e.into();
    let msg = err.to_string();
    let status = if msg.contains("destination namespace must be empty")
        || msg.contains("cannot copy namespace to itself")
    {
        StatusCode::BAD_REQUEST
    } else if msg.contains("source namespace not found") {
        StatusCode::NOT_FOUND
    } else {
        return storage_error_response(err);
    };
    (status, Json(serde_json::json!({"error": msg}))).into_response()
}

fn storage_error_response(e: impl Into<anyhow::Error>) -> axum::response::Response {
    let err: anyhow::Error = e.into();
    let (status, message) = match s3_error_hint(&err) {
        Some("bucket") => (
            StatusCode::SERVICE_UNAVAILABLE,
            "S3 bucket not found".to_string(),
        ),
        Some("invalid_object_name") => (
            StatusCode::BAD_REQUEST,
            "document id is not valid for object storage".to_string(),
        ),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    (
        status,
        Json(serde_json::json!({"error": message})),
    )
        .into_response()
}