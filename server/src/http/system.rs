use crate::configs::http::HttpMetricsConfig;
use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::mapper;
use crate::http::shared::AppState;
use crate::http::COMPONENT;
use crate::streaming::session::Session;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use bytes::Bytes;
use chrono::Local;
use error_set::ErrContext;
use iggy::locking::IggySharedMutFn;
use iggy::models::client_info::{ClientInfo, ClientInfoDetails};
use iggy::models::stats::Stats;
use iggy::system::get_snapshot::GetSnapshot;
use iggy::validatable::Validatable;
use std::sync::Arc;

const NAME: &str = "Iggy API";
const PONG: &str = "pong";

pub fn router(state: Arc<AppState>, metrics_config: &HttpMetricsConfig) -> Router {
    let mut router = Router::new()
        .route("/", get(|| async { NAME }))
        .route("/ping", get(|| async { PONG }))
        .route("/stats", get(get_stats))
        .route("/clients", get(get_clients))
        .route("/clients/{client_id}", get(get_client))
        .route("/snapshot", post(get_snapshot));
    if metrics_config.enabled {
        router = router.route(&metrics_config.endpoint, get(get_metrics));
    }

    router.with_state(state)
}

async fn get_metrics(State(state): State<Arc<AppState>>) -> Result<String, CustomError> {
    let system = state.system.read().await;
    Ok(system.metrics.get_formatted_output())
}

async fn get_stats(State(state): State<Arc<AppState>>) -> Result<Json<Stats>, CustomError> {
    let system = state.system.read().await;
    let stats = system.get_stats().await.with_error_context(|error| {
        format!("{COMPONENT} (error: {error}) - failed to get stats")
    })?;
    Ok(Json(stats))
}

async fn get_client(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(client_id): Path<u32>,
) -> Result<Json<ClientInfoDetails>, CustomError> {
    let system = state.system.read().await;
    let Ok(client) = system
        .get_client(
            &Session::stateless(identity.user_id, identity.ip_address),
            client_id,
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to get client, user ID: {}",
                identity.user_id
            )
        })
    else {
        return Err(CustomError::ResourceNotFound);
    };
    let Some(client) = client else {
        return Err(CustomError::ResourceNotFound);
    };

    let client = client.read().await;
    let client = mapper::map_client(&client);
    Ok(Json(client))
}

async fn get_clients(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<ClientInfo>>, CustomError> {
    let system = state.system.read().await;
    let clients = system
        .get_clients(&Session::stateless(identity.user_id, identity.ip_address))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to get clients, user ID: {}",
                identity.user_id
            )
        })?;
    let clients = mapper::map_clients(&clients).await;
    Ok(Json(clients))
}

async fn get_snapshot(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Json(command): Json<GetSnapshot>,
) -> Result<impl IntoResponse, CustomError> {
    command.validate()?;

    let session = Session::stateless(identity.user_id, identity.ip_address);
    let system = state.system.read().await;

    let snapshot = system
        .get_snapshot(&session, command.compression, &command.snapshot_types)
        .await?;

    let zip_data = Bytes::from(snapshot.0);
    let filename = format!("iggy_snapshot_{}.zip", Local::now().format("%Y%m%d_%H%M%S"));

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/zip"),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        header::HeaderValue::from_str(&format!("attachment; filename=\"{}\"", filename)).unwrap(),
    );
    Ok((headers, Body::from(zip_data)))
}
