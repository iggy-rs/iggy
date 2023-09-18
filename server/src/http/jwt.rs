use crate::http::claims::JwtClaims;
use crate::http::state::AppState;
use axum::{
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use iggy::error::Error;
use iggy::utils::timestamp::TimeStamp;
use jsonwebtoken::{encode, Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation};
use std::sync::Arc;

const AUDIENCE: &str = "iggy.rs";
const AUTHORIZATION: &str = "authorization";
const BEARER: &str = "Bearer ";

#[derive(Debug, Clone)]
pub struct Identity {
    pub user_id: u32,
}

pub async fn jwt_auth<T>(
    State(state): State<Arc<AppState>>,
    mut request: Request<T>,
    next: Next<T>,
) -> Result<Response, StatusCode> {
    match request.uri().path() {
        "/" => return Ok(next.run(request).await),
        "/ping" => return Ok(next.run(request).await),
        "/users/login" => return Ok(next.run(request).await),
        _ => (),
    }

    let header = match request.headers().get(AUTHORIZATION) {
        Some(v) => v,
        None => return Err(StatusCode::UNAUTHORIZED),
    };

    let bearer = match header.to_str() {
        Ok(v) => v,
        Err(_) => return Err(StatusCode::UNAUTHORIZED),
    };

    if !bearer.starts_with(BEARER) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let jwt_token = bearer.trim_start_matches(BEARER);
    let token_header = match jsonwebtoken::decode_header(jwt_token) {
        Ok(header) => header,
        _ => return Err(StatusCode::UNAUTHORIZED),
    };

    let jwt_claims = match state.jwt_manager.decode(jwt_token, token_header.alg) {
        Ok(claims) => claims,
        _ => return Err(StatusCode::UNAUTHORIZED),
    };

    let identity = Identity {
        user_id: jwt_claims.claims.sub,
    };
    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}

pub async fn no_jwt_auth<T>(
    mut request: Request<T>,
    next: Next<T>,
) -> Result<Response, StatusCode> {
    request.extensions_mut().insert(Identity { user_id: 0 });
    Ok(next.run(request).await)
}

pub struct JwtManager {
    expiry: u64,
    decoding_key: DecodingKey,
    encoding_key: EncodingKey,
}

impl JwtManager {
    pub fn new(secret: &str, expiry: u64) -> Self {
        Self {
            expiry,
            decoding_key: DecodingKey::from_secret(secret.as_ref()),
            encoding_key: EncodingKey::from_secret(secret.as_ref()),
        }
    }

    pub fn generate(&self, user_id: u32) -> String {
        let header = Header::new(Algorithm::HS256);
        let iat = TimeStamp::now().to_micros();
        let exp = iat + 1_000_000 * self.expiry;
        let claims = JwtClaims {
            sub: user_id,
            aud: AUDIENCE.to_string(),
            iat,
            exp,
        };

        encode::<JwtClaims>(&header, &claims, &self.encoding_key).unwrap()
    }

    pub fn decode(&self, token: &str, algorithm: Algorithm) -> Result<TokenData<JwtClaims>, Error> {
        match jsonwebtoken::decode::<JwtClaims>(
            token,
            &self.decoding_key,
            &Validation::new(algorithm),
        ) {
            Ok(claims) => Ok(claims),
            _ => Err(Error::Unauthenticated),
        }
    }
}
