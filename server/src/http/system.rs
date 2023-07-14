use crate::http::error::CustomError;
use crate::http::mapper;
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use iggy::models::client_info::{ClientInfo, ClientInfoDetails};
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
        .route("/clients/:client_id", get(get_client))
        .with_state(system);
    #[cfg(feature = "allow_kill_command")]
    {
        router.route("/kill", axum::routing::post(kill))
    }

    #[cfg(not(feature = "allow_kill_command"))]
    router
}

async fn get_client(
    State(system): State<Arc<RwLock<System>>>,
    Path(client_id): Path<u32>,
) -> Result<Json<ClientInfoDetails>, CustomError> {
    let system = system.read().await;
    let client = system.get_client(client_id).await?;
    let client = client.read().await;
    let client = mapper::map_client(&client).await;
    Ok(Json(client))
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
