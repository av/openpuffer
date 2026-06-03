use crate::config::AppConfig;
use crate::export::MAX_EXPORT_LIMIT;
use crate::limits::{self, validate_namespace_name};
use crate::models::{
    validate_doc_id, ApiErrorResponse, Document, ExportRequest, ExportResponse, HealthResponse,
    NamespaceSummary, NamespacesResponse, QueryRequest, RecallRequest, RecallResponse,
    WriteRequest,
};
use crate::schema::{
    merge_schema, validate_and_normalize_document_attributes, validate_patch_attributes,
};
use crate::filter::parse_filter;
use crate::meta::{parse_distance_metric, resolve_distance_metric};
use crate::storage::s3_error_hint;
use crate::search;
use crate::storage::Storage;
use axum::{
    body::Body,
    extract::{rejection::JsonRejection, DefaultBodyLimit, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use crate::models::QueryPerformance;
use std::sync::Arc;
use tracing::error;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<Storage>,
    pub config: AppConfig,
}

/// turbopuffer-style `{"error": "...", "status": "error"}` with the given HTTP status.
fn api_error(status: StatusCode, message: impl Into<String>) -> axum::response::Response {
    (status, Json(ApiErrorResponse::new(message))).into_response()
}

/// Map Axum JSON body rejections to the same error shape as handler validation errors.
fn json_rejection_response(err: JsonRejection) -> axum::response::Response {
    let msg = match err {
        JsonRejection::MissingJsonContentType(_) => {
            "Expected request Content-Type: application/json".to_string()
        }
        JsonRejection::JsonSyntaxError(_) => "Invalid JSON in request body".to_string(),
        JsonRejection::JsonDataError(e) => format!("Invalid JSON field: {e}"),
        JsonRejection::BytesRejection(_) => "Failed to read request body".to_string(),
        _ => "Invalid JSON request".to_string(),
    };
    api_error(StatusCode::BAD_REQUEST, msg)
}

pub fn router(state: AppState) -> Router {
    let r = Router::new()
        .route("/health", get(health));

    #[cfg(feature = "metrics")]
    let r = r.route("/metrics", get(prometheus_metrics));

    let r = r
        .route("/v1/namespaces", get(list_namespaces))
        .route("/v1/namespaces/{name}", get(get_namespace_metadata))
        .route(
            "/v1/namespaces/{name}/export",
            get(export_namespace_get).post(export_namespace_post),
        )
        .route("/v1/namespaces/{name}/warm", post(warm_namespace_handler))
        .route("/v1/namespaces/{name}/recall", post(recall_namespace_handler))
        .route("/v2/namespaces/{name}", post(write_namespace))
        .route("/v2/namespaces/{name}/query", post(query_namespace))
        .route("/v2/namespaces/{name}", delete(delete_namespace));

    #[cfg(feature = "integration")]
    let r = r
        .route("/v1/debug/cache-stats", get(cache_stats_debug))
        .route("/v1/debug/cache-stats/reset", post(cache_stats_reset_debug));

    r.layer(middleware::from_fn(reject_oversized_write_body))
        .layer(DefaultBodyLimit::max(limits::MAX_WRITE_BODY_BYTES))
        .with_state(state)
}

async fn reject_oversized_write_body(req: Request<Body>, next: Next) -> Response {
    if let Some(v) = req.headers().get(header::CONTENT_LENGTH) {
        if let Ok(s) = v.to_str() {
            if let Ok(n) = s.parse::<usize>() {
                if n > limits::MAX_WRITE_BODY_BYTES {
                    return api_error(
                        StatusCode::BAD_REQUEST,
                        format!(
                            "request body exceeds maximum write size of {} MiB",
                            limits::MAX_WRITE_BODY_BYTES / (1024 * 1024)
                        ),
                    );
                }
            }
        }
    }
    next.run(req).await
}

fn namespace_name_error_response(name: &str) -> Option<axum::response::Response> {
    validate_namespace_name(name)
        .err()
        .map(|msg| api_error(StatusCode::BAD_REQUEST, msg))
}

#[derive(Debug, Default, serde::Deserialize)]
struct HealthQuery {
    #[serde(default)]
    deep: Option<u8>,
}

#[cfg(feature = "metrics")]
async fn prometheus_metrics() -> impl IntoResponse {
    match crate::metrics::render() {
        Ok(body) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(e) => {
            error!("prometheus encode: {e}");
            api_error(StatusCode::INTERNAL_SERVER_ERROR, "metrics encode failed")
        }
    }
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
    body: Result<Option<Json<ExportRequest>>, JsonRejection>,
) -> impl IntoResponse {
    let req = match body {
        Ok(Some(Json(b))) => b,
        Ok(None) => ExportRequest::default(),
        Err(e) => return json_rejection_response(e),
    };
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
    if let Some(resp) = namespace_name_error_response(name) {
        return resp;
    }
    if let Some(id) = last_id {
        if let Err(msg) = validate_doc_id(id) {
            return api_error(StatusCode::BAD_REQUEST, msg);
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
                return api_error(StatusCode::NOT_FOUND, "namespace not found");
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
                    return api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
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

async fn recall_namespace_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Result<Option<Json<RecallRequest>>, JsonRejection>,
) -> impl IntoResponse {
    if let Some(resp) = namespace_name_error_response(&name) {
        return resp;
    }
    let req = match body {
        Ok(Some(Json(b))) => b,
        Ok(None) => RecallRequest::default(),
        Err(e) => return json_rejection_response(e),
    };
    if req.num == 0 {
        return api_error(StatusCode::BAD_REQUEST, "num must be at least 1");
    }
    if req.top_k == 0 {
        return api_error(StatusCode::BAD_REQUEST, "top_k must be at least 1");
    }
    if let Err(e) = state.storage.require_namespace(&name).await {
        if e.to_string().contains("namespace not found") {
            return api_error(StatusCode::NOT_FOUND, "namespace not found");
        }
        error!("recall namespace existence {name}: {e:#}");
        return storage_error_response(e);
    }
    match state
        .storage
        .load_namespace_for_query(&name, search::QueryConsistency::Strong)
        .await
    {
        Ok(mut loaded) => {
            if let Err(e) = state
                .storage
                .load_vector_indexes_full_for_eval(&name, &mut loaded)
                .await
            {
                error!("recall load vectors {name}: {e:#}");
                return storage_error_response(e);
            }
            let field = match crate::recall::recall_vector_field(&loaded) {
                Ok(f) => f,
                Err(e) => return api_error(StatusCode::BAD_REQUEST, e.to_string()),
            };
            let use_rerank = state.config.ann_rerank;
            match crate::recall::measure_recall_for_loaded(
                &loaded,
                &field,
                req.num,
                req.top_k,
                use_rerank,
                req.filters.as_ref(),
                &name,
            ) {
                Ok(metrics) => (StatusCode::OK, Json(RecallResponse::from(metrics))).into_response(),
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("no vector index") || msg.contains("no documents") {
                        return api_error(StatusCode::BAD_REQUEST, msg);
                    }
                    error!("recall evaluate {name}: {e:#}");
                    storage_error_response(e)
                }
            }
        }
        Err(e) => {
            error!("recall load {name}: {e:#}");
            if e.to_string().contains("namespace not found") {
                return api_error(StatusCode::NOT_FOUND, "namespace not found");
            }
            storage_error_response(e)
        }
    }
}

async fn warm_namespace_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Some(resp) = namespace_name_error_response(&name) {
        return resp;
    }
    match state.storage.warm_namespace(&name).await {
        Ok(stats) => (StatusCode::OK, Json(stats)).into_response(),
        Err(e) => {
            error!("warm namespace {name}: {e:#}");
            if e.to_string().contains("namespace not found") {
                return api_error(StatusCode::NOT_FOUND, "namespace not found");
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
    if let Some(resp) = namespace_name_error_response(&name) {
        return resp;
    }
    match state.storage.namespace_metadata(&name).await {
        Ok(meta) => (StatusCode::OK, Json(meta)).into_response(),
        Err(e) => {
            error!("namespace metadata {name}: {e:#}");
            if e.to_string().contains("namespace not found") {
                return api_error(StatusCode::NOT_FOUND, "namespace not found");
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
        || body
            .patch_by_filter
            .as_ref()
            .is_some_and(|v| !v.is_null())
}

async fn write_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Result<Json<WriteRequest>, JsonRejection>,
) -> impl IntoResponse {
    let Json(body) = match body {
        Ok(b) => b,
        Err(e) => return json_rejection_response(e),
    };
    if let Some(resp) = namespace_name_error_response(&name) {
        return resp;
    }

    if body.copy_from_namespace.is_some() && body.branch_from_namespace.is_some() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "copy_from_namespace and branch_from_namespace are mutually exclusive",
        );
    }

    if let Some(source) = body.branch_from_namespace.as_deref() {
        if let Some(resp) = namespace_name_error_response(source) {
            return resp;
        }
        return handle_namespace_s3_clone(
            &state,
            &name,
            source,
            "branch_from_namespace",
            &body,
            NamespaceS3CloneOp::Branch,
        )
        .await;
    }

    if let Some(source) = body.copy_from_namespace.as_deref() {
        if let Some(resp) = namespace_name_error_response(source) {
            return resp;
        }
        return handle_namespace_s3_clone(
            &state,
            &name,
            source,
            "copy_from_namespace",
            &body,
            NamespaceS3CloneOp::Copy,
        )
        .await;
    }

    let explicit_rows = limits::count_explicit_write_rows(&body);
    if explicit_rows > state.config.limits.max_upsert_rows {
        return api_error(
            StatusCode::BAD_REQUEST,
            format!(
                "write request has {explicit_rows} rows; maximum is {}",
                state.config.limits.max_upsert_rows
            ),
        );
    }

    let mut upserts = Vec::new();
    for row in body.upsert_rows {
        if let Err(msg) = validate_doc_id(&row.id) {
            return api_error(StatusCode::BAD_REQUEST, msg);
        }
        upserts.push(Document {
            id: row.id,
            attributes: row.attributes,
        });
    }
    for id in &body.deletes {
        if let Err(msg) = validate_doc_id(id) {
            return api_error(StatusCode::BAD_REQUEST, msg);
        }
    }

    if let Some(cols) = body.upsert_columns {
        match apply_column_batch(&mut upserts, cols, false) {
            Ok(()) => {}
            Err(err) => {
                return api_error(StatusCode::BAD_REQUEST, err);
            }
        }
    }

    let mut patches = Vec::new();
    for row in body.patch_rows {
        if let Err(msg) = validate_doc_id(&row.id) {
            return api_error(StatusCode::BAD_REQUEST, msg);
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
                return api_error(StatusCode::BAD_REQUEST, err);
            }
        }
    }

    let mut patch_by_filter = match body
        .patch_by_filter
        .as_ref()
        .filter(|v| !v.is_null())
    {
        None => None,
        Some(v) => match parse_patch_by_filter(v) {
            Ok(parsed) => Some(parsed),
            Err(msg) => {
                return api_error(StatusCode::BAD_REQUEST, msg);
            }
        },
    };

    let effective_schema = match effective_write_schema(
        &state,
        &name,
        body.schema.as_ref(),
        !upserts.is_empty() || !patches.is_empty() || patch_by_filter.is_some(),
    )
    .await
    {
        Ok(s) => s,
        Err(msg) => {
            return api_error(StatusCode::BAD_REQUEST, msg);
        }
    };

    if let Some((_, ref patch_attrs)) = patch_by_filter {
        if let Err(msg) = validate_patch_attributes(patch_attrs, &effective_schema) {
            return api_error(StatusCode::BAD_REQUEST, msg);
        }
    }

    for patch in &patches {
        if let Err(msg) = validate_patch_attributes(&patch.attributes, &effective_schema) {
            return api_error(StatusCode::BAD_REQUEST, msg);
        }
    }

    if let Some((ref filters, _)) = patch_by_filter {
        if let Err(e) = parse_filter(filters) {
            return api_error(StatusCode::BAD_REQUEST, format!("{e:#}"));
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
            Err(e) => return api_error(StatusCode::BAD_REQUEST, format!("{e:#}")),
        },
    };

    let patch_condition = match body
        .patch_condition
        .as_ref()
        .filter(|v| !v.is_null())
    {
        None => None,
        Some(v) => match parse_filter(v) {
            Ok(_) => body.patch_condition.clone(),
            Err(e) => return api_error(StatusCode::BAD_REQUEST, format!("{e:#}")),
        },
    };

    let delete_condition = match body
        .delete_condition
        .as_ref()
        .filter(|v| !v.is_null())
    {
        None => None,
        Some(v) => match parse_filter(v) {
            Ok(_) => body.delete_condition.clone(),
            Err(e) => return api_error(StatusCode::BAD_REQUEST, format!("{e:#}")),
        },
    };

    let distance_metric = match body.distance_metric.as_deref() {
        None => None,
        Some(s) if s.is_empty() => None,
        Some(s) => match parse_distance_metric(s) {
            Ok(m) => Some(m),
            Err(e) => return api_error(StatusCode::BAD_REQUEST, format!("{e:#}")),
        },
    };

    let norm_meta = crate::meta::NamespaceMeta {
        schema: effective_schema.clone(),
        ..Default::default()
    };
    for doc in &mut upserts {
        if let Err(e) =
            crate::vector_encoding::normalize_document_vectors(&mut doc.attributes, &norm_meta)
        {
            return api_error(StatusCode::BAD_REQUEST, format!("{e:#}"));
        }
        if let Err(msg) =
            validate_and_normalize_document_attributes(&mut doc.attributes, &effective_schema)
        {
            return api_error(StatusCode::BAD_REQUEST, msg);
        }
    }

    for patch in &mut patches {
        if let Err(e) =
            crate::vector_encoding::normalize_document_vectors(&mut patch.attributes, &norm_meta)
        {
            return api_error(StatusCode::BAD_REQUEST, format!("{e:#}"));
        }
        if let Err(msg) =
            validate_and_normalize_document_attributes(&mut patch.attributes, &effective_schema)
        {
            return api_error(StatusCode::BAD_REQUEST, msg);
        }
    }

    if let Some((_, ref mut patch_attrs)) = patch_by_filter {
        if let Err(msg) =
            validate_and_normalize_document_attributes(patch_attrs, &effective_schema)
        {
            return api_error(StatusCode::BAD_REQUEST, msg);
        }
    }

    if let Some(metric) = distance_metric {
        if let Ok(Some((meta, _))) = crate::namespace::fetch_meta(
            state.storage.client(),
            state.storage.bucket(),
            &name,
        )
        .await
        {
            if let Err(e) = resolve_distance_metric(&meta, Some(metric)) {
                return api_error(StatusCode::BAD_REQUEST, format!("{e:#}"));
            }
        }
    }

    match state
        .storage
        .write_documents(
            &name,
            upserts,
            patches,
            body.deletes,
            body.schema,
            body.delete_by_filter,
            body.delete_by_filter_allow_partial,
            patch_by_filter,
            body.patch_by_filter_allow_partial,
            upsert_condition,
            patch_condition,
            delete_condition,
            distance_metric,
            body.return_affected_ids,
        )
        .await
    {
        Ok(stats) => {
            if body.block_until_indexed {
                if let Err(e) = state.storage.wait_until_indexed(&name).await {
                    error!("block_until_indexed {name}: {e:#}");
                    let msg = e.to_string();
                    if msg.contains("did not catch up") {
                        return api_error(StatusCode::GATEWAY_TIMEOUT, msg);
                    }
                    return storage_error_response(e);
                }
            }
            (
                StatusCode::OK,
                Json(crate::models::WriteResponse::from_stats(name, stats)),
            )
                .into_response()
        }
        Err(e) => {
            error!("write namespace {name}: {e:#}");
            let msg = e.to_string();
            if msg.contains("distance_metric") || msg.contains("filter matched") {
                return api_error(StatusCode::BAD_REQUEST, msg);
            }
            storage_error_response(e)
        }
    }
}

async fn effective_write_schema(
    state: &AppState,
    namespace: &str,
    request_schema: Option<&serde_json::Value>,
    needs_existing_schema: bool,
) -> Result<serde_json::Value, String> {
    if request_schema.is_none() && !needs_existing_schema {
        return Ok(serde_json::json!({}));
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
        Some(patch) => merge_schema(&base, patch).map_err(|e| format!("{e:#}")),
        None => Ok(base),
    }
}

/// turbopuffer `patch_by_filter`: `{ "filters": <filter>, "patch": { ...attrs } }`.
fn parse_patch_by_filter(
    value: &serde_json::Value,
) -> Result<(serde_json::Value, std::collections::HashMap<String, serde_json::Value>), String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "patch_by_filter must be an object".to_string())?;
    let filters = obj
        .get("filters")
        .cloned()
        .ok_or_else(|| "patch_by_filter requires filters".to_string())?;
    let patch_val = obj
        .get("patch")
        .ok_or_else(|| "patch_by_filter requires patch".to_string())?;
    let patch_obj = patch_val
        .as_object()
        .ok_or_else(|| "patch_by_filter.patch must be an object".to_string())?;
    let patch = patch_obj
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    Ok((filters, patch))
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
    body: Result<Json<QueryRequest>, JsonRejection>,
) -> impl IntoResponse {
    let Json(body) = match body {
        Ok(b) => b,
        Err(e) => return json_rejection_response(e),
    };
    if let Some(resp) = namespace_name_error_response(&name) {
        return resp;
    }
    if let Err(e) = state.storage.require_namespace(&name).await {
        if e.to_string().contains("namespace not found") {
            return api_error(StatusCode::NOT_FOUND, "namespace not found");
        }
        error!("query namespace existence {name}: {e:#}");
        return storage_error_response(e);
    }
    let consistency = match search::QueryConsistency::parse(body.consistency.as_deref()) {
        Ok(c) => c,
        Err(e) => return api_error(StatusCode::BAD_REQUEST, e.to_string()),
    };
    match state
        .storage
        .load_namespace_for_query(&name, consistency)
        .await
    {
        Ok(mut loaded) => {
            if let Ok(probes) = search::vector_probe_specs(&body.rank_by) {
                if let Err(e) = state
                    .storage
                    .finish_cold_vector_probes(&name, &mut loaded, &probes)
                    .await
                {
                    error!("query cold vector probe {name}: {e:#}");
                    return storage_error_response(e);
                }
            }
            let ctx = search::QueryContext {
                docs: &loaded.docs,
                meta: &loaded.meta,
                fts: loaded.fts.as_ref(),
                vectors: &loaded.vectors,
                filter_index: loaded.filter_index.as_ref(),
                tail_doc_ids: &loaded.tail_doc_ids,
                consistency,
                storage_roundtrips: loaded.storage_roundtrips,
                ann_rerank: Some(state.config.ann_rerank),
            };
            match search::execute_query(&ctx, &body) {
            Ok(resp) => {
                let headers = query_performance_headers(resp.performance.as_ref());
                (StatusCode::OK, headers, Json(resp)).into_response()
            }
            Err(e) => api_error(StatusCode::BAD_REQUEST, e.to_string()),
            }
        }
        Err(e) => {
            error!("query load {name}: {e:#}");
            if e.to_string().contains("namespace not found") {
                return api_error(StatusCode::NOT_FOUND, "namespace not found");
            }
            storage_error_response(e)
        }
    }
}

async fn delete_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Some(resp) = namespace_name_error_response(&name) {
        return resp;
    }
    match state.storage.delete_namespace(&name).await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "deleted", "namespace": name})),
        )
            .into_response(),
        Err(e) => {
            error!("delete namespace {name}: {e:#}");
            if e.to_string().contains("namespace not found") {
                return api_error(StatusCode::NOT_FOUND, "namespace not found");
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

enum NamespaceS3CloneOp {
    Copy,
    Branch,
}

async fn handle_namespace_s3_clone(
    state: &AppState,
    dest: &str,
    source: &str,
    field: &str,
    body: &WriteRequest,
    op: NamespaceS3CloneOp,
) -> axum::response::Response {
    if source.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            format!("{field} must not be empty"),
        );
    }
    if write_has_row_ops(body) {
        return api_error(
            StatusCode::BAD_REQUEST,
            format!("{field} cannot be combined with other write operations"),
        );
    }
    let result = match op {
        NamespaceS3CloneOp::Copy => state.storage.copy_from_namespace(dest, source).await,
        NamespaceS3CloneOp::Branch => state.storage.branch_from_namespace(dest, source).await,
    };
    match result {
        Ok(()) => (
            StatusCode::OK,
            Json(crate::models::WriteResponse::from_stats(
                dest.to_string(),
                crate::models::WriteStats::default(),
            )),
        )
            .into_response(),
        Err(e) => {
            error!("{field} {dest} from {source}: {e:#}");
            copy_namespace_error_response(e)
        }
    }
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
    api_error(status, msg)
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
    api_error(status, message)
}