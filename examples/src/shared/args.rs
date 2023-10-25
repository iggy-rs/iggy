use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "0")]
    pub message_batches_limit: u64,

    #[arg(long, default_value = "iggy")]
    pub username: String,

    #[arg(long, default_value = "iggy")]
    pub password: String,

    #[arg(long, default_value = "1000")]
    pub interval: u64,

    #[arg(long, default_value = "9999")]
    pub stream_id: u32,

    #[arg(long, default_value = "1")]
    pub topic_id: u32,

    #[arg(long, default_value = "1")]
    pub partition_id: u32,

    #[arg(long, default_value = "1")]
    pub partitions_count: u32,

    #[arg(long, default_value = "1")]
    pub consumer_kind: u8,

    #[arg(long, default_value = "0")]
    pub consumer_id: u32,

    #[arg(long, default_value = "1")]
    pub messages_per_batch: u32,

    #[arg(long, default_value = "tcp")]
    pub transport: String,

    #[arg(long, default_value = "")]
    pub encryption_key: String,

    #[arg(long, default_value = "http://localhost:3000")]
    pub http_api_url: String,

    #[arg(long, default_value = "3")]
    pub http_retries: u32,

    #[arg(long, default_value = "3")]
    pub tcp_reconnection_retries: u32,

    #[arg(long, default_value = "1000")]
    pub tcp_reconnection_interval: u64,

    #[arg(long, default_value = "127.0.0.1:8090")]
    pub tcp_server_address: String,

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

impl Args {
    pub fn to_sdk_args(&self) -> iggy::args::Args {
        iggy::args::Args {
            transport: self.transport.clone(),
            encryption_key: self.encryption_key.clone(),
            http_api_url: self.http_api_url.clone(),
            http_retries: self.http_retries,
            tcp_server_address: self.tcp_server_address.clone(),
            tcp_reconnection_retries: self.tcp_reconnection_retries,
            tcp_reconnection_interval: self.tcp_reconnection_interval,
            tcp_tls_enabled: self.tcp_tls_enabled,
            tcp_tls_domain: self.tcp_tls_domain.clone(),
            quic_client_address: self.quic_client_address.clone(),
            quic_server_address: self.quic_server_address.clone(),
            quic_server_name: self.quic_server_name.clone(),
            quic_reconnection_retries: self.quic_reconnection_retries,
            quic_reconnection_interval: self.quic_reconnection_interval,
            quic_max_concurrent_bidi_streams: self.quic_max_concurrent_bidi_streams,
            quic_datagram_send_buffer_size: self.quic_datagram_send_buffer_size,
            quic_initial_mtu: self.quic_initial_mtu,
            quic_send_window: self.quic_send_window,
            quic_receive_window: self.quic_receive_window,
            quic_response_buffer_size: self.quic_response_buffer_size,
            quic_keep_alive_interval: self.quic_keep_alive_interval,
            quic_max_idle_timeout: self.quic_max_idle_timeout,
            quic_validate_certificate: self.quic_validate_certificate,
        }
    }
}
