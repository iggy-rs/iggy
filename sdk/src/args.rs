use clap::Parser;
use serde::{Deserialize, Serialize};

/// The arguments used by the `ClientProviderConfig` to create a client.
/// We are not using default values here because we want to be able to
/// distinguish between the default value and the value that was not
/// provided by the user. This allows us to fallback to values provided
/// in contexts.toml.
#[derive(Parser, Debug, Clone, Deserialize, Serialize, Default)]
#[command(author, version, about, long_about = None)]
pub struct ArgsOptional {
    /// The transport to use. Valid values are `http` and `tcp`
    ///
    /// [default: tcp]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<String>,

    /// Optional encryption key for the message payload used by the client
    ///
    /// [default: ]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_key: Option<String>,

    /// The optional API URL for the HTTP transport
    ///
    /// [default: http://localhost:3000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_api_url: Option<String>,

    /// The optional number of retries for the HTTP transport
    ///
    /// [default: 3]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_retries: Option<u32>,

    /// The optional client address for the TCP transport
    ///
    /// [default: 127.0.0.1:8090]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_server_address: Option<String>,

    /// The optional number of reconnect retries for the TCP transport
    ///
    /// [default: 3]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_reconnection_retries: Option<u32>,

    /// The optional reconnect interval for the TCP transport
    ///
    /// [default: 1000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_reconnection_interval: Option<u64>,

    /// Flag to enable TLS for the TCP transport
    #[arg(long, default_missing_value(Some("true")), num_args(0..1))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_tls_enabled: Option<bool>,

    /// The optional TLS domain for the TCP transport
    ///
    /// [default: localhost]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_tls_domain: Option<String>,
}

/// The arguments used by the `ClientProviderConfig` to create a client.
#[derive(Debug, Clone)]
pub struct Args {
    /// The transport to use. Valid values are `http` and `tcp`
    pub transport: String,

    /// Optional encryption key for the message payload used by the client
    pub encryption_key: String,

    /// The optional API URL for the HTTP transport
    pub http_api_url: String,

    /// The optional number of retries for the HTTP transport
    pub http_retries: u32,

    /// The optional client address for the TCP transport
    pub tcp_server_address: String,

    /// The optional number of reconnect retries for the TCP transport
    pub tcp_reconnection_retries: u32,

    /// The optional reconnect interval for the TCP transport
    pub tcp_reconnection_interval: u64,

    /// Flag to enable TLS for the TCP transport
    pub tcp_tls_enabled: bool,

    /// The optional TLS domain for the TCP transport
    pub tcp_tls_domain: String,
}

const HTTP_TRANSPORT: &str = "http";
const TCP_TRANSPORT: &str = "tcp";

impl Args {
    pub fn get_server_address(&self) -> Option<String> {
        match self.transport.as_str() {
            HTTP_TRANSPORT => Some(
                self.http_api_url
                    .clone()
                    .replace("http://", "")
                    .replace("localhost", "127.0.0.1")
                    .split(':')
                    .next()
                    .unwrap()
                    .into(),
            ),
            TCP_TRANSPORT => Some(self.tcp_server_address.split(':').next().unwrap().into()),
            _ => None,
        }
    }
}

impl Default for Args {
    fn default() -> Self {
        Self {
            transport: "tcp".to_string(),
            encryption_key: "".to_string(),
            http_api_url: "http://localhost:3000".to_string(),
            http_retries: 3,
            tcp_server_address: "127.0.0.1:8090".to_string(),
            tcp_reconnection_retries: 3,
            tcp_reconnection_interval: 1000,
            tcp_tls_enabled: false,
            tcp_tls_domain: "localhost".to_string(),
        }
    }
}

impl From<Vec<ArgsOptional>> for Args {
    fn from(args_set: Vec<ArgsOptional>) -> Self {
        let mut args = Args::default();

        for optional_args in args_set {
            if let Some(transport) = optional_args.transport {
                args.transport = transport;
            }
            if let Some(encryption_key) = optional_args.encryption_key {
                args.encryption_key = encryption_key;
            }
            if let Some(http_api_url) = optional_args.http_api_url {
                args.http_api_url = http_api_url;
            }
            if let Some(http_retries) = optional_args.http_retries {
                args.http_retries = http_retries;
            }
            if let Some(tcp_server_address) = optional_args.tcp_server_address {
                args.tcp_server_address = tcp_server_address;
            }
            if let Some(tcp_reconnection_retries) = optional_args.tcp_reconnection_retries {
                args.tcp_reconnection_retries = tcp_reconnection_retries;
            }
            if let Some(tcp_reconnection_interval) = optional_args.tcp_reconnection_interval {
                args.tcp_reconnection_interval = tcp_reconnection_interval;
            }
            if let Some(tcp_tls_enabled) = optional_args.tcp_tls_enabled {
                args.tcp_tls_enabled = tcp_tls_enabled;
            }
            if let Some(tcp_tls_domain) = optional_args.tcp_tls_domain {
                args.tcp_tls_domain = tcp_tls_domain;
            }
        }

        args
    }
}
