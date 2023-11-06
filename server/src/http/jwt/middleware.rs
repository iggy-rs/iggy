use crate::http::jwt::json_web_token::Identity;
use crate::http::state::AppState;
use axum::extract::ConnectInfo;
use axum::{
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::debug;

const AUTHORIZATION: &str = "authorization";
const BEARER: &str = "Bearer ";
const UNAUTHORIZED: StatusCode = StatusCode::UNAUTHORIZED;

const UNAUTHORIZED_PATHS: &[&str] = &[
    "/",
    "/metrics",
    "/ping",
    "/users/login",
    "/users/refresh-token",
    "/personal-access-tokens/login",
];

pub async fn jwt_auth<T>(
    State(state): State<Arc<AppState>>,
    ConnectInfo(ip_address): ConnectInfo<SocketAddr>,
    mut request: Request<T>,
    next: Next<T>,
) -> Result<Response, StatusCode> {
    debug!(
        "Processing request {} {} from client with IP address: {ip_address}.",
        request.method(),
        request.uri().path()
    );

    if UNAUTHORIZED_PATHS.contains(&request.uri().path()) {
        return Ok(next.run(request).await);
    }

    let bearer = request
        .headers()
        .get(AUTHORIZATION)
        .ok_or(UNAUTHORIZED)?
        .to_str()
        .map_err(|_| UNAUTHORIZED)?;

    if !bearer.starts_with(BEARER) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let jwt_token = &bearer[BEARER.len()..];
    let token_header = jsonwebtoken::decode_header(jwt_token).map_err(|_| UNAUTHORIZED)?;
    let jwt_claims = state
        .jwt_manager
        .decode(jwt_token, token_header.alg)
        .map_err(|_| UNAUTHORIZED)?;
    if state
        .jwt_manager
        .is_token_revoked(&jwt_claims.claims.jti)
        .await
    {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let identity = Identity {
        token_id: jwt_claims.claims.jti,
        token_expiry: jwt_claims.claims.exp,
        user_id: jwt_claims.claims.sub,
        ip_address: ip_address.to_string(),
    };
    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}
