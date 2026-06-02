use crate::config::AppConfig;
use crate::models::{
    Document, HealthResponse, NamespaceSummary, NamespacesResponse, QueryRequest, WriteRequest,
};
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
            let namespaces = names
                .into_iter()
                .map(|id| NamespaceSummary { id })
                .collect();
            (StatusCode::OK, Json(NamespacesResponse { namespaces })).into_response()
        }
        Err(e) => {
            error!("list namespaces: {e:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response()
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
        upserts.push(Document {
            id: row.id,
            attributes: row.attributes,
        });
    }

    if let Some(cols) = body.upsert_columns {
        match apply_upsert_columns(&mut upserts, cols) {
            Ok(()) => {}
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": err})),
                );
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
        ),
        Err(e) => {
            error!("write namespace {name}: {e:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
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
        Ok(loaded) => match search::execute_query(&loaded.docs, &body) {
            Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response(),
        },
        Err(e) => {
            error!("query load {name}: {e:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response()
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
        ),
        Err(e) => {
            error!("delete namespace {name}: {e:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
        }
    }
}