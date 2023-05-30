use crate::http::{messages, streams, system, topics};
use axum::Router;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;

use tracing::info;

pub async fn start(address: String, system: Arc<RwLock<System>>) {
    info!("Starting HTTP API on: {:?}", address);
    let app = Router::new().nest("/", system::router()).nest(
        "/streams",
        streams::router(system.clone()).nest(
            "/:stream_id/topics",
            topics::router(system.clone())
                .nest("/:topic_id/messages", messages::router(system.clone())),
        ),
    );

    axum::Server::bind(&address.parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
