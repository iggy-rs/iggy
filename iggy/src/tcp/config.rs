#[derive(Debug, Clone)]
pub struct TcpClientConfig {
    pub server_address: String,
    pub reconnection_retries: u32,
    pub reconnection_interval: u64,
}

impl Default for TcpClientConfig {
    fn default() -> TcpClientConfig {
        TcpClientConfig {
            server_address: "127.0.0.1:8090".to_string(),
            reconnection_retries: 3,
            reconnection_interval: 1000,
        }
    }
}
