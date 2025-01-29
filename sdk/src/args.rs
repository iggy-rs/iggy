use crate::users::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
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
    /// The transport to use. Valid values are `quick`, `http` and `tcp`
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

    /// Optional username for initial login
    ///
    /// [default: DEFAULT_ROOT_USERNAME]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_username: Option<String>,

    /// Optional password for initial login
    ///
    /// [default: DEFAULT_ROOT_PASSWORD]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_password: Option<String>,

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

    /// The optional number of max reconnect retries for the TCP transport
    ///
    /// [default: 3]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_reconnection_max_retries: Option<u32>,

    /// The optional reconnect interval for the TCP transport
    ///
    /// [default: "1s"]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_reconnection_interval: Option<String>,

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

    /// The optional client address for the quick transport
    ///
    /// [default: 127.0.0.1:0]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_client_address: Option<String>,

    /// The optional server address for the quick transport
    ///
    /// [default: 127.0.0.1:8080]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_server_address: Option<String>,

    /// The optional server name for the quick transport
    ///
    /// [default: localhost]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_server_name: Option<String>,

    /// The optional number of max reconnect retries for the quick transport
    ///
    /// [default: 3]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_reconnection_max_retries: Option<u32>,

    /// The optional reconnect interval for the quick transport
    ///
    /// [default: "1s"]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_reconnection_interval: Option<String>,

    /// The optional maximum number of concurrent bidirectional streams for quick
    ///
    /// [default: 10000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_max_concurrent_bidi_streams: Option<u64>,

    /// The optional datagram send buffer size for quick
    ///
    /// [default: 100000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_datagram_send_buffer_size: Option<u64>,

    /// The optional initial MTU for quick
    ///
    /// [default: 1200]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_initial_mtu: Option<u16>,

    /// The optional send window for quick
    ///
    /// [default: 100000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_send_window: Option<u64>,

    /// The optional receive window for quick
    ///
    /// [default: 100000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_receive_window: Option<u64>,

    /// The optional response buffer size for quick
    ///
    /// [default: 1048576]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_response_buffer_size: Option<u64>,

    /// The optional keep alive interval for quick
    ///
    /// [default: 5000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_keep_alive_interval: Option<u64>,

    /// The optional maximum idle timeout for quick
    ///
    /// [default: 10000]
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_max_idle_timeout: Option<u64>,

    /// Flag to enable certificate validation for quick
    #[arg(long, default_missing_value(Some("true")), num_args(0..1))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quick_validate_certificate: Option<bool>,
}

/// The arguments used by the `ClientProviderConfig` to create a client.
#[derive(Debug, Clone)]
pub struct Args {
    /// The transport to use. Valid values are `quick`, `http` and `tcp`
    pub transport: String,

    /// Optional encryption key for the message payload used by the client
    pub encryption_key: String,

    /// The optional API URL for the HTTP transport
    pub http_api_url: String,

    /// The optional number of retries for the HTTP transport
    pub http_retries: u32,

    // The optional username for initial login
    pub username: String,

    // The optional password for initial login
    pub password: String,

    /// The optional client address for the TCP transport
    pub tcp_server_address: String,

    /// The optional number of maximum reconnect retries for the TCP transport
    pub tcp_reconnection_enabled: bool,

    /// The optional number of maximum reconnect retries for the TCP transport
    pub tcp_reconnection_max_retries: Option<u32>,

    /// The optional reconnect interval for the TCP transport
    pub tcp_reconnection_interval: String,

    /// The optional re-establish after last connection interval for TCP
    pub tcp_reconnection_reestablish_after: String,

    /// The optional heartbeat interval for the TCP transport
    pub tcp_heartbeat_interval: String,

    /// Flag to enable TLS for the TCP transport
    pub tcp_tls_enabled: bool,

    /// The optional TLS domain for the TCP transport
    pub tcp_tls_domain: String,

    /// The optional CA file for the TCP transport
    pub tcp_tls_ca_file: Option<String>,

    /// The optional client address for the quick transport
    pub quick_client_address: String,

    /// The optional server address for the quick transport
    pub quick_server_address: String,

    /// The optional server name for the quick transport
    pub quick_server_name: String,

    /// The optional number of maximum reconnect retries for the quick transport
    pub quick_reconnection_enabled: bool,

    /// The optional number of maximum reconnect retries for the quick transport
    pub quick_reconnection_max_retries: Option<u32>,

    /// The optional reconnect interval for the quick transport
    pub quick_reconnection_interval: String,

    /// The optional re-establish after last connection interval for quick
    pub quick_reconnection_reestablish_after: String,

    /// The optional maximum number of concurrent bidirectional streams for quick
    pub quick_max_concurrent_bidi_streams: u64,

    /// The optional datagram send buffer size for quick
    pub quick_datagram_send_buffer_size: u64,

    /// The optional initial MTU for quick
    pub quick_initial_mtu: u16,

    /// The optional send window for quick
    pub quick_send_window: u64,

    /// The optional receive window for quick
    pub quick_receive_window: u64,

    /// The optional response buffer size for quick
    pub quick_response_buffer_size: u64,

    /// The optional keep alive interval for quick
    pub quick_keep_alive_interval: u64,

    /// The optional maximum idle timeout for quick
    pub quick_max_idle_timeout: u64,

    /// Flag to enable certificate validation for quick
    pub quick_validate_certificate: bool,

    /// The optional heartbeat interval for the quick transport
    pub quick_heartbeat_interval: String,
}

const quick_TRANSPORT: &str = "quick";
const HTTP_TRANSPORT: &str = "http";
const TCP_TRANSPORT: &str = "tcp";

impl Args {
    pub fn get_server_address(&self) -> Option<String> {
        match self.transport.as_str() {
            quick_TRANSPORT => Some(self.quick_server_address.replace("localhost", "127.0.0.1")),
            HTTP_TRANSPORT => Some(
                self.http_api_url
                    .clone()
                    .replace("http://", "")
                    .replace("localhost", "127.0.0.1"),
            ),
            TCP_TRANSPORT => Some(self.tcp_server_address.replace("localhost", "127.0.0.1")),
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
            username: DEFAULT_ROOT_USERNAME.to_string(),
            password: DEFAULT_ROOT_PASSWORD.to_string(),
            tcp_server_address: "127.0.0.1:8090".to_string(),
            tcp_reconnection_enabled: true,
            tcp_reconnection_max_retries: None,
            tcp_reconnection_interval: "1s".to_string(),
            tcp_reconnection_reestablish_after: "5s".to_string(),
            tcp_heartbeat_interval: "5s".to_string(),
            tcp_tls_enabled: false,
            tcp_tls_domain: "localhost".to_string(),
            tcp_tls_ca_file: None,
            quick_client_address: "127.0.0.1:0".to_string(),
            quick_server_address: "127.0.0.1:8080".to_string(),
            quick_server_name: "localhost".to_string(),
            quick_reconnection_enabled: true,
            quick_reconnection_max_retries: None,
            quick_reconnection_interval: "1s".to_string(),
            quick_reconnection_reestablish_after: "5s".to_string(),
            quick_max_concurrent_bidi_streams: 10000,
            quick_datagram_send_buffer_size: 100000,
            quick_initial_mtu: 1200,
            quick_send_window: 100000,
            quick_receive_window: 100000,
            quick_response_buffer_size: 1048576,
            quick_keep_alive_interval: 5000,
            quick_max_idle_timeout: 10000,
            quick_validate_certificate: false,
            quick_heartbeat_interval: "5s".to_string(),
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
            if let Some(username) = optional_args.credentials_username {
                args.username = username;
            }
            if let Some(password) = optional_args.credentials_password {
                args.password = password;
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
            if let Some(tcp_reconnection_retries) = optional_args.tcp_reconnection_max_retries {
                args.tcp_reconnection_max_retries = Some(tcp_reconnection_retries);
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
            if let Some(quick_client_address) = optional_args.quick_client_address {
                args.quick_client_address = quick_client_address;
            }
            if let Some(quick_server_address) = optional_args.quick_server_address {
                args.quick_server_address = quick_server_address;
            }
            if let Some(quick_server_name) = optional_args.quick_server_name {
                args.quick_server_name = quick_server_name;
            }
            if let Some(quick_reconnection_retries) = optional_args.quick_reconnection_max_retries {
                args.quick_reconnection_max_retries = Some(quick_reconnection_retries);
            }
            if let Some(quick_reconnection_interval) = optional_args.quick_reconnection_interval {
                args.quick_reconnection_interval = quick_reconnection_interval;
            }
            if let Some(quick_max_concurrent_bidi_streams) =
                optional_args.quick_max_concurrent_bidi_streams
            {
                args.quick_max_concurrent_bidi_streams = quick_max_concurrent_bidi_streams;
            }
            if let Some(quick_datagram_send_buffer_size) =
                optional_args.quick_datagram_send_buffer_size
            {
                args.quick_datagram_send_buffer_size = quick_datagram_send_buffer_size;
            }
            if let Some(quick_initial_mtu) = optional_args.quick_initial_mtu {
                args.quick_initial_mtu = quick_initial_mtu;
            }
            if let Some(quick_send_window) = optional_args.quick_send_window {
                args.quick_send_window = quick_send_window;
            }
            if let Some(quick_receive_window) = optional_args.quick_receive_window {
                args.quick_receive_window = quick_receive_window;
            }
            if let Some(quick_response_buffer_size) = optional_args.quick_response_buffer_size {
                args.quick_response_buffer_size = quick_response_buffer_size;
            }
            if let Some(quick_keep_alive_interval) = optional_args.quick_keep_alive_interval {
                args.quick_keep_alive_interval = quick_keep_alive_interval;
            }
            if let Some(quick_max_idle_timeout) = optional_args.quick_max_idle_timeout {
                args.quick_max_idle_timeout = quick_max_idle_timeout;
            }
            if let Some(quick_validate_certificate) = optional_args.quick_validate_certificate {
                args.quick_validate_certificate = quick_validate_certificate;
            }
        }

        args
    }
}
