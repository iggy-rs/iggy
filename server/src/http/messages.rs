use std::sync::Arc;
use axum::Router;
use tokio::sync::Mutex;
use streaming::system::System;

pub fn router(system: Arc<Mutex<System>>) -> Router {
    Router::new()
        .with_state(system)
}