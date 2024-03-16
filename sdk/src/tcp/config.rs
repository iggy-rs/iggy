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

/// Builder for the TCP client configuration.
/// Allows configuring the TCP client with custom settings or using defaults:
/// - `server_address`: Default is "127.0.0.1:8090"
/// - `reconnection_retries`: Default is 3.
/// - `reconnection_interval`: Default is 1000 ms.
/// - `tls_enabled`: Default is false.
/// - `tls_domain`: Default is "localhost".
#[derive(Debug, Default)]
pub struct TcpClientConfigBuilder {
    config: TcpClientConfig,
}

impl TcpClientConfigBuilder {
    pub fn new() -> Self {
        TcpClientConfigBuilder::default()
    }

    /// Sets the server address for the TCP client.
    pub fn with_server_address(mut self, server_address: String) -> Self {
        self.config.server_address = server_address;
        self
    }

    /// Sets the number of retries when connecting to the server.
    pub fn with_reconnection_retries(mut self, reconnection_retries: u32) -> Self {
        self.config.reconnection_retries = reconnection_retries;
        self
    }

    /// Sets the interval between retries when connecting to the server.
    pub fn with_reconnection_interval(mut self, reconnection_interval: u64) -> Self {
        self.config.reconnection_interval = reconnection_interval;
        self
    }

    /// Sets whether to use TLS when connecting to the server.
    pub fn with_tls_enabled(mut self, tls_enabled: bool) -> Self {
        self.config.tls_enabled = tls_enabled;
        self
    }

    /// Sets the domain to use for TLS when connecting to the server.
    pub fn with_tls_domain(mut self, tls_domain: String) -> Self {
        self.config.tls_domain = tls_domain;
        self
    }

    /// Builds the TCP client configuration.
    pub fn build(self) -> TcpClientConfig {
        self.config
    }
}
