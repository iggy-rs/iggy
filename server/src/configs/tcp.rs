use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct TcpConfig {
    pub enabled: bool,
    pub address: String,
    pub tls: TcpTlsConfig,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct TcpTlsConfig {
    pub enabled: bool,
    pub certificate: String,
    pub password: String,
}
