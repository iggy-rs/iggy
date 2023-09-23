use crate::configs::http::HttpMetricsConfig;
use crate::http::error::CustomError;
use crate::http::jwt::Identity;
use crate::http::mapper;
use crate::http::state::AppState;
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Extension, Json, Router};
use iggy::models::client_info::{ClientInfo, ClientInfoDetails};
use iggy::models::stats::Stats;
use std::sync::Arc;

const NAME: &str = "Iggy HTTP";
const PONG: &str = "pong";

pub fn router(state: Arc<AppState>, metrics_config: &HttpMetricsConfig) -> Router {
    let mut router = Router::new()
        .route("/", get(|| async { NAME }))
        .route("/ping", get(|| async { PONG }))
        .route("/stats", get(get_stats))
        .route("/clients", get(get_clients))
        .route("/clients/:client_id", get(get_client));
    if metrics_config.enabled {
        router = router.route(&metrics_config.endpoint, get(get_metrics));
    }

    router.with_state(state)
}

async fn get_metrics(State(state): State<Arc<AppState>>) -> Result<String, CustomError> {
    let system = state.system.read().await;
    Ok(system.metrics.get_formatted_output())
}

async fn get_stats(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Stats>, CustomError> {
    let system = state.system.read().await;
    system.permissioner.get_stats(identity.user_id)?;
    let stats = system.get_stats().await;
    Ok(Json(stats))
}

async fn get_client(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(client_id): Path<u32>,
) -> Result<Json<ClientInfoDetails>, CustomError> {
    let system = state.system.read().await;
    system.permissioner.get_client(identity.user_id)?;
    let client = system.get_client(client_id).await?;
    let client = client.read().await;
    let client = mapper::map_client(&client).await;
    Ok(Json(client))
}

async fn get_clients(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<ClientInfo>>, CustomError> {
    let system = state.system.read().await;
    system.permissioner.get_clients(identity.user_id)?;
    let clients = system.get_clients().await;
    let clients = mapper::map_clients(&clients).await;
    Ok(Json(clients))
}
