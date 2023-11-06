use iggy::models::user_info::UserId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct Identity {
    pub token_id: String,
    pub token_expiry: u64,
    pub user_id: UserId,
    pub ip_address: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JwtClaims {
    pub jti: String,
    pub iss: String,
    pub aud: String,
    pub sub: u32,
    pub iat: u64,
    pub exp: u64,
    pub nbf: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RevokedAccessToken {
    pub id: String,
    pub expiry: u64,
}

#[derive(Debug)]
pub struct GeneratedTokens {
    pub user_id: UserId,
    pub access_token: String,
    pub refresh_token: String,
    pub expiry: u64,
}
