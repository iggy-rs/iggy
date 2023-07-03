use crate::http::error::CustomError;
use crate::http::mapper;
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use sdk::models::client_info::ClientInfo;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;

const NAME: &str = "Iggy HTTP";
const PONG: &str = "pong";

pub fn router(system: Arc<RwLock<System>>) -> Router {
    let router = Router::new()
        .route("/", get(|| async { NAME }))
        .route("/ping", get(|| async { PONG }))
        .route("/clients", get(get_clients))
        .with_state(system);
    #[cfg(feature = "allow_kill_command")]
    {
        router.route("/kill", axum::routing::post(kill))
    }

    #[cfg(not(feature = "allow_kill_command"))]
    router
}

async fn get_clients(
    State(system): State<Arc<RwLock<System>>>,
) -> Result<Json<Vec<ClientInfo>>, CustomError> {
    let system = system.read().await;
    let clients = system.get_clients().await;
    let clients = mapper::map_clients(&clients).await;
    Ok(Json(clients))
}

#[cfg(feature = "allow_kill_command")]
async fn kill() -> axum::http::StatusCode {
    tokio::spawn(async move { std::process::exit(0) });
    axum::http::StatusCode::OK
}
