use iggy::error::Error;
use jsonwebtoken::Algorithm;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct HttpConfig {
    pub enabled: bool,
    pub address: String,
    pub cors: HttpCorsConfig,
    pub jwt: HttpJwtConfig,
    pub metrics: HttpMetricsConfig,
    pub tls: HttpTlsConfig,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct HttpCorsConfig {
    pub enabled: bool,
    pub allowed_methods: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub exposed_headers: Vec<String>,
    pub allow_credentials: bool,
    pub allow_private_network: bool,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct HttpJwtConfig {
    pub algorithm: String,
    pub audience: String,
    pub expiry: u64,
    pub encoding_secret: String,
    pub decoding_secret: String,
    pub use_base64_secret: bool,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct HttpMetricsConfig {
    pub enabled: bool,
    pub endpoint: String,
}

#[derive(Debug)]
pub enum JwtSecret {
    Default(String),
    Base64(String),
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct HttpTlsConfig {
    pub enabled: bool,
    pub cert_file: String,
    pub key_file: String,
}

impl HttpJwtConfig {
    pub fn get_algorithm(&self) -> Result<Algorithm, Error> {
        match self.algorithm.as_str() {
            "HS256" => Ok(Algorithm::HS256),
            "HS384" => Ok(Algorithm::HS384),
            "HS512" => Ok(Algorithm::HS512),
            "RS256" => Ok(Algorithm::RS256),
            "RS384" => Ok(Algorithm::RS384),
            "RS512" => Ok(Algorithm::RS512),
            _ => Err(Error::InvalidJwtAlgorithm(self.algorithm.clone())),
        }
    }

    pub fn get_decoding_secret(&self) -> JwtSecret {
        self.get_secret(&self.decoding_secret)
    }

    pub fn get_encoding_secret(&self) -> JwtSecret {
        self.get_secret(&self.encoding_secret)
    }

    fn get_secret(&self, secret: &str) -> JwtSecret {
        if self.use_base64_secret {
            JwtSecret::Base64(secret.to_string())
        } else {
            JwtSecret::Default(secret.to_string())
        }
    }
}
