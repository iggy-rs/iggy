use crate::configs::http::{HttpJwtConfig, JwtSecret};
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
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const AUTHORIZATION: &str = "authorization";
const BEARER: &str = "Bearer ";
const UNAUTHORIZED: StatusCode = StatusCode::UNAUTHORIZED;

#[derive(Debug, Clone)]
pub struct Identity {
    pub user_id: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JwtClaims {
    pub sub: u32,
    pub iat: u64,
    pub aud: String,
    pub exp: u64,
}

pub async fn jwt_auth<T>(
    State(state): State<Arc<AppState>>,
    mut request: Request<T>,
    next: Next<T>,
) -> Result<Response, StatusCode> {
    if should_skip_auth(request.uri().path()) {
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
    algorithm: Algorithm,
    audience: String,
    expiry: u64,
    decoding_key: DecodingKey,
    encoding_key: EncodingKey,
}

impl JwtManager {
    pub fn new(
        algorithm: Algorithm,
        audience: &str,
        expiry: u64,
        decoding_secret: JwtSecret,
        encoding_secret: JwtSecret,
    ) -> Result<Self, Error> {
        let decoding_key = match decoding_secret {
            JwtSecret::Default(ref secret) => DecodingKey::from_secret(secret.as_ref()),
            JwtSecret::Base64(ref secret) => {
                DecodingKey::from_base64_secret(secret).map_err(|_| Error::InvalidJwtSecret)?
            }
        };

        let encoding_key = match encoding_secret {
            JwtSecret::Default(ref secret) => EncodingKey::from_secret(secret.as_ref()),
            JwtSecret::Base64(ref secret) => {
                EncodingKey::from_base64_secret(secret).map_err(|_| Error::InvalidJwtSecret)?
            }
        };

        Ok(Self {
            algorithm,
            audience: audience.to_string(),
            expiry,
            decoding_key,
            encoding_key,
        })
    }

    pub fn from_config(config: &HttpJwtConfig) -> Result<Self, Error> {
        if config.encoding_secret.is_empty() {
            return Err(Error::InvalidJwtSecret);
        }

        let algorithm = config.get_algorithm()?;
        let decoding_secret = config.get_decoding_secret();
        let encoding_secret = config.get_encoding_secret();
        JwtManager::new(
            algorithm,
            &config.audience,
            config.expiry,
            decoding_secret,
            encoding_secret,
        )
    }

    pub fn generate(&self, user_id: u32) -> String {
        let header = Header::new(self.algorithm);
        let iat = TimeStamp::now().to_micros();
        let exp = iat + 1_000_000 * self.expiry;
        let claims = JwtClaims {
            sub: user_id,
            aud: self.audience.to_string(),
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

fn should_skip_auth(path: &str) -> bool {
    matches!(path, "/" | "/ping" | "/users/login")
}
