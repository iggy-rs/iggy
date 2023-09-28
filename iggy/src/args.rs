use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "tcp")]
    pub transport: String,

    #[cfg(feature = "credentials-in-args")]
    #[arg(long, default_value = "iggy")]
    pub username: String,

    #[cfg(feature = "credentials-in-args")]
    #[arg(long, default_value = "iggy")]
    pub password: String,

    #[arg(long, default_value = "")]
    pub encryption_key: String,

    #[arg(long, default_value = "http://localhost:3000")]
    pub http_api_url: String,

    #[arg(long, default_value = "3")]
    pub http_retries: u32,

    #[arg(long, default_value = "127.0.0.1:8090")]
    pub tcp_server_address: String,

    #[arg(long, default_value = "3")]
    pub tcp_reconnection_retries: u32,

    #[arg(long, default_value = "1000")]
    pub tcp_reconnection_interval: u64,

    #[arg(long, default_value = "false")]
    pub tcp_tls_enabled: bool,

    #[arg(long, default_value = "localhost")]
    pub tcp_tls_domain: String,

    #[arg(long, default_value = "127.0.0.1:0")]
    pub quic_client_address: String,

    #[arg(long, default_value = "127.0.0.1:8080")]
    pub quic_server_address: String,

    #[arg(long, default_value = "localhost")]
    pub quic_server_name: String,

    #[arg(long, default_value = "3")]
    pub quic_reconnection_retries: u32,

    #[arg(long, default_value = "1000")]
    pub quic_reconnection_interval: u64,

    #[arg(long, default_value = "10000")]
    pub quic_max_concurrent_bidi_streams: u64,

    #[arg(long, default_value = "100000")]
    pub quic_datagram_send_buffer_size: u64,

    #[arg(long, default_value = "1200")]
    pub quic_initial_mtu: u16,

    #[arg(long, default_value = "100000")]
    pub quic_send_window: u64,

    #[arg(long, default_value = "100000")]
    pub quic_receive_window: u64,

    #[arg(long, default_value = "1048576")]
    pub quic_response_buffer_size: u64,

    #[arg(long, default_value = "5000")]
    pub quic_keep_alive_interval: u64,

    #[arg(long, default_value = "10000")]
    pub quic_max_idle_timeout: u64,

    #[arg(long, default_value = "false")]
    pub quic_validate_certificate: bool,
}
