use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct HttpConfig {
    pub enabled: bool,
    pub address: String,
    pub cors: HttpCorsConfig,
    pub jwt: JwtConfig,
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
pub struct JwtConfig {
    pub secret: String,
    pub expiry: u64,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct HttpTlsConfig {
    pub enabled: bool,
    pub cert_file: String,
    pub key_file: String,
}
