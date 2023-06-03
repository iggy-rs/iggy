use crate::http::{messages, streams, system, topics};
use axum::Router;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;

use crate::server_config::HttpConfig;
use tracing::info;

pub async fn start(config: HttpConfig, system: Arc<RwLock<System>>) {
    info!("Starting HTTP API on: {:?}", config.address);
    let app = Router::new().nest("/", system::router()).nest(
        "/streams",
        streams::router(system.clone()).nest(
            "/:stream_id/topics",
            topics::router(system.clone())
                .nest("/:topic_id/messages", messages::router(system.clone())),
        ),
    );

    axum::Server::bind(&config.address.parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
