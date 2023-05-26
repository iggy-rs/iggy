use crate::http::streams;
use axum::routing::get;
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
        .nest("/streams", streams::router(system.clone()));

    axum::Server::bind(&address.parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
