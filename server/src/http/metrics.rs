use crate::http::shared::AppState;
use axum::{
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use std::sync::Arc;

pub async fn metrics<T>(
    State(state): State<Arc<AppState>>,
    request: Request<T>,
    next: Next<T>,
) -> Result<Response, StatusCode> {
    state.system.read().await.metrics.increment_http_requests();
    Ok(next.run(request).await)
}
