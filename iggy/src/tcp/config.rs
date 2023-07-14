#[derive(Debug, Clone)]
pub struct TcpClientConfig {
    pub server_address: String,
}

impl Default for TcpClientConfig {
    fn default() -> TcpClientConfig {
        TcpClientConfig {
            server_address: "127.0.0.1:8090".to_string(),
        }
    }
}
