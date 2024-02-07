use crate::configs::http::HttpMetricsConfig;
use crate::http::error::CustomError;
use crate::http::jwt::json_web_token::Identity;
use crate::http::mapper;
use crate::http::shared::AppState;
use crate::streaming::session::Session;
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Extension, Json, Router};
use iggy::locking::IggySharedMutFn;
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
    let system = state.system.read();
    Ok(system.metrics.get_formatted_output())
}

async fn get_stats(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Stats>, CustomError> {
    let system = state.system.read();
    let stats = system
        .get_stats(&Session::stateless(identity.user_id, identity.ip_address))
        .await?;
    Ok(Json(stats))
}

async fn get_client(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
    Path(client_id): Path<u32>,
) -> Result<Json<ClientInfoDetails>, CustomError> {
    let system = state.system.read();
    let client = system
        .get_client(
            &Session::stateless(identity.user_id, identity.ip_address),
            client_id,
        )
        .await?;
    let client = client.read().await;
    let client = mapper::map_client(&client).await;
    Ok(Json(client))
}

async fn get_clients(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<Identity>,
) -> Result<Json<Vec<ClientInfo>>, CustomError> {
    let system = state.system.read();
    let clients = system
        .get_clients(&Session::stateless(identity.user_id, identity.ip_address))
        .await?;
    let clients = mapper::map_clients(&clients).await;
    Ok(Json(clients))
}
