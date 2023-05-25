use crate::http::streams::{create_stream, delete_stream, get_streams};
use axum::routing::{delete, get};
use axum::Router;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::Mutex;
use tracing::info;

const NAME: &str = "Iggy HTTP";

pub async fn start(address: String, system: Arc<Mutex<System>>) {
    info!("Starting HTTP API on: {}", address);
    let app = Router::new()
        .route("/", get(|| async { NAME }))
        .route("/streams", get(get_streams).post(create_stream))
        .route("/streams/:id", delete(delete_stream))
        .with_state(system);

    axum::Server::bind(&address.parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
