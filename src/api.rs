use crate::config::AppConfig;
use crate::models::{
    validate_doc_id, Document, HealthResponse, NamespaceSummary,
    NamespacesResponse, QueryRequest, WriteRequest,
};
use crate::storage::s3_error_hint;
use crate::search;
use crate::storage::Storage;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use std::sync::Arc;
use tracing::error;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<Storage>,
    pub config: AppConfig,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/namespaces", get(list_namespaces))
        .route("/v1/namespaces/{name}", get(get_namespace_metadata))
        .route("/v2/namespaces/{name}", post(write_namespace))
        .route("/v2/namespaces/{name}/query", post(query_namespace))
        .route("/v2/namespaces/{name}", delete(delete_namespace))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

async fn list_namespaces(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.list_namespaces().await {
        Ok(names) => {
            let mut namespaces = Vec::with_capacity(names.len());
            for id in names {
                let summary = match state.storage.namespace_metadata(&id).await {
                    Ok(meta) => NamespaceSummary {
                        id,
                        index_cursor: Some(meta.index_cursor),
                        wal_commit_seq: Some(meta.wal_commit_seq),
                        unindexed_bytes: Some(meta.unindexed_bytes),
                    },
                    Err(_) => NamespaceSummary {
                        id,
                        index_cursor: None,
                        wal_commit_seq: None,
                        unindexed_bytes: None,
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

async fn write_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<WriteRequest>,
) -> impl IntoResponse {
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
        match apply_upsert_columns(&mut upserts, cols) {
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

    match state
        .storage
        .write_documents(&name, upserts, body.deletes)
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "ok", "namespace": name})),
        )
            .into_response(),
        Err(e) => {
            error!("write namespace {name}: {e:#}");
            storage_error_response(e)
        }
    }
}

fn apply_upsert_columns(
    upserts: &mut Vec<Document>,
    cols: serde_json::Value,
) -> Result<(), String> {
    let obj = cols
        .as_object()
        .ok_or_else(|| "upsert_columns must be an object".to_string())?;
    let id_col = obj
        .get("id")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "upsert_columns requires id column".to_string())?;
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
        upserts.push(Document { id, attributes: attrs });
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
                tail_doc_ids: &loaded.tail_doc_ids,
            };
            match search::execute_query(&ctx, &body) {
            Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
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