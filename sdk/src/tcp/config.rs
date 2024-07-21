use crate::client::AutoSignIn;
use crate::utils::duration::IggyDuration;
use std::str::FromStr;

/// Configuration for the TCP client.
#[derive(Debug, Clone)]
pub struct TcpClientConfig {
    /// The address of the Iggy server.
    pub server_address: String,
    /// Whether to use TLS when connecting to the server.
    pub tls_enabled: bool,
    /// The domain to use for TLS when connecting to the server.
    pub tls_domain: String,
    // Whether to automatically sign in when connecting.
    pub auto_sign_in: AutoSignIn,
    // Whether to automatically reconnect when disconnected.
    pub reconnection: TcpClientReconnectionConfig,
}

#[derive(Debug, Clone)]
pub struct TcpClientReconnectionConfig {
    pub enabled: bool,
    pub max_retries: Option<u32>,
    pub interval: IggyDuration,
}

impl Default for TcpClientConfig {
    fn default() -> TcpClientConfig {
        TcpClientConfig {
            server_address: "127.0.0.1:8090".to_string(),
            tls_enabled: false,
            tls_domain: "localhost".to_string(),
            auto_sign_in: AutoSignIn::Disabled,
            reconnection: TcpClientReconnectionConfig::default(),
        }
    }
}

impl Default for TcpClientReconnectionConfig {
    fn default() -> TcpClientReconnectionConfig {
        TcpClientReconnectionConfig {
            enabled: true,
            max_retries: None,
            interval: IggyDuration::from_str("1s").unwrap(),
        }
    }
}

/// Builder for the TCP client configuration.
/// Allows configuring the TCP client with custom settings or using defaults:
/// - `server_address`: Default is "127.0.0.1:8090"
/// - `auto_sign_in`: Default is AutoSignIn::Disabled.
/// - `reconnection`: Default is enabled unlimited retries and 1 second interval.
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

    /// Sets the auto sign in during connection.
    pub fn with_auto_sign_in(mut self, auto_sign_in: AutoSignIn) -> Self {
        self.config.auto_sign_in = auto_sign_in;
        self
    }

    pub fn with_enabled_reconnection(mut self) -> Self {
        self.config.reconnection.enabled = true;
        self
    }

    /// Sets the number of retries when connecting to the server.
    pub fn with_reconnection_max_retries(mut self, max_retries: Option<u32>) -> Self {
        self.config.reconnection.max_retries = max_retries;
        self
    }

    /// Sets the interval between retries when connecting to the server.
    pub fn with_reconnection_interval(mut self, interval: IggyDuration) -> Self {
        self.config.reconnection.interval = interval;
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
