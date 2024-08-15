use crate::http::shared::RequestDetails;
use crate::streaming::utils::random_id;
use axum::body::Body;
use axum::{
    extract::ConnectInfo,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use std::net::SocketAddr;
use tokio::time::Instant;
use tracing::{debug, error};

pub async fn request_diagnostics(
    ConnectInfo(ip_address): ConnectInfo<SocketAddr>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let request_id = random_id::get_ulid();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    debug!(
        "Processing a request {} {} with ID: {request_id} from client with IP address: {ip_address}...",
        request.method(),
        path_and_query,
    );
    request.extensions_mut().insert(RequestDetails {
        request_id,
        ip_address,
    });
    let now = Instant::now();
    let result = Ok(next.run(request).await);
    if let Ok(response) = &result {
        let status = response.status();
        if status != StatusCode::NOT_FOUND && status >= StatusCode::BAD_REQUEST {
            error!("Returning an invalid status code: {status}, IP address: {ip_address}, request ID: {request_id}");
        }
    }
    let elapsed = now.elapsed();
    debug!(
        "Processed a request with ID: {request_id} from client with IP address: {ip_address} in {} ms.",
        elapsed.as_millis()
    );
    result
}
