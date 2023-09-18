use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct JwtClaims {
    pub sub: u32,
    pub iat: u64,
    pub aud: String,
    pub exp: u64,
}
