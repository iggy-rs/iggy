use crate::http::shared::AppState;
use axum::body::Body;
use axum::{
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use iggy::locking::IggySharedMutFn;
use std::sync::Arc;

pub async fn metrics(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    state
        .system
        .read()
        .metrics
        .write()
        .await
        .increment_http_requests();
    Ok(next.run(request).await)
}
