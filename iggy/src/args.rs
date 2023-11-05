use clap::Parser;

/// The arguments used by the `ClientProviderConfig` to create a client.
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// The transport to use. Valid values are `quic`, `http` and `tcp`.
    #[arg(long, default_value = "tcp")]
    pub transport: String,

    /// Optional encryption key for the message payload used by the client.
    #[arg(long, default_value = "")]
    pub encryption_key: String,

    /// The optional API URL for the HTTP transport.
    #[arg(long, default_value = "http://localhost:3000")]
    pub http_api_url: String,

    /// The optional number of retries for the HTTP transport.
    #[arg(long, default_value = "3")]
    pub http_retries: u32,

    /// The optional client address for the TCP transport.
    #[arg(long, default_value = "127.0.0.1:8090")]
    pub tcp_server_address: String,

    /// The optional number of reconnect retries for the TCP transport.
    #[arg(long, default_value = "3")]
    pub tcp_reconnection_retries: u32,

    /// The optional reconnect interval for the TCP transport.
    #[arg(long, default_value = "1000")]
    pub tcp_reconnection_interval: u64,

    /// Flag to enable TLS for the TCP transport.
    #[arg(long, default_value = "false")]
    pub tcp_tls_enabled: bool,

    /// The optional TLS domain for the TCP transport.
    #[arg(long, default_value = "localhost")]
    pub tcp_tls_domain: String,

    /// The optional client address for the QUIC transport.
    #[arg(long, default_value = "127.0.0.1:0")]
    pub quic_client_address: String,

    /// The optional server address for the QUIC transport.
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub quic_server_address: String,

    /// The optional server name for the QUIC transport.
    #[arg(long, default_value = "localhost")]
    pub quic_server_name: String,

    /// The optional number of reconnect retries for the QUIC transport.
    #[arg(long, default_value = "3")]
    pub quic_reconnection_retries: u32,

    /// The optional reconnect interval for the QUIC transport.
    #[arg(long, default_value = "1000")]
    pub quic_reconnection_interval: u64,

    /// The optional maximum number of concurrent bidirectional streams for QUIC.
    #[arg(long, default_value = "10000")]
    pub quic_max_concurrent_bidi_streams: u64,

    /// The optional datagram send buffer size for QUIC.
    #[arg(long, default_value = "100000")]
    pub quic_datagram_send_buffer_size: u64,

    /// The optional initial MTU for QUIC.
    #[arg(long, default_value = "1200")]
    pub quic_initial_mtu: u16,

    /// The optional send window for QUIC.
    #[arg(long, default_value = "100000")]
    pub quic_send_window: u64,

    /// The optional receive window for QUIC.
    #[arg(long, default_value = "100000")]
    pub quic_receive_window: u64,

    /// The optional response buffer size for QUIC.
    #[arg(long, default_value = "1048576")]
    pub quic_response_buffer_size: u64,

    /// The optional keep alive interval for QUIC.
    #[arg(long, default_value = "5000")]
    pub quic_keep_alive_interval: u64,

    /// The optional maximum idle timeout for QUIC.
    #[arg(long, default_value = "10000")]
    pub quic_max_idle_timeout: u64,

    /// Flag to enable certificate validation for QUIC.
    #[arg(long, default_value = "false")]
    pub quic_validate_certificate: bool,
}
