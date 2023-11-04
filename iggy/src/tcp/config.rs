/// Configuration for the TCP client.
#[derive(Debug, Clone)]
pub struct TcpClientConfig {
    /// The address of the Iggy server.
    pub server_address: String,
    /// The number of retries when connecting to the server.
    pub reconnection_retries: u32,
    /// The interval between retries when connecting to the server.
    pub reconnection_interval: u64,
    /// Whether to use TLS when connecting to the server.
    pub tls_enabled: bool,
    /// The domain to use for TLS when connecting to the server.
    pub tls_domain: String,
}

impl Default for TcpClientConfig {
    fn default() -> TcpClientConfig {
        TcpClientConfig {
            server_address: "127.0.0.1:8090".to_string(),
            reconnection_retries: 3,
            reconnection_interval: 1000,
            tls_enabled: false,
            tls_domain: "localhost".to_string(),
        }
    }
}
