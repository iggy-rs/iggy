use clap::Parser;
use iggy::users::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use iggy::utils::duration::IggyDuration;
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "10")]
    pub message_batches_limit: u64,

    #[arg(long, default_value = DEFAULT_ROOT_USERNAME)]
    pub username: String,

    #[arg(long, default_value = DEFAULT_ROOT_PASSWORD)]
    pub password: String,

    #[arg(long, default_value = "1ms")]
    pub interval: String,

    #[arg(long, default_value = "example-stream")]
    pub stream_id: String,

    #[arg(long, default_value = "example-topic")]
    pub topic_id: String,

    #[arg(long, default_value = "1")]
    pub partition_id: u32,

    #[arg(long, default_value = "1")]
    pub partitions_count: u32,

    #[arg(long, default_value = "1")]
    pub compression_algorithm: u8,

    #[arg(long, default_value = "1")]
    pub consumer_kind: u8,

    #[arg(long, default_value = "1")]
    pub consumer_id: u32,

    #[arg(long, default_value = "1")]
    pub messages_per_batch: u32,

    #[arg(long, default_value = "0")]
    pub offset: u64,

    #[arg(long, default_value = "false")]
    pub auto_commit: bool,

    #[arg(long, default_value = "tcp")]
    pub transport: String,

    #[arg(long, default_value = "")]
    pub encryption_key: String,

    #[arg(long, default_value = "http://localhost:3000")]
    pub http_api_url: String,

    #[arg(long, default_value = "3")]
    pub http_retries: u32,

    #[arg(long, default_value = "true")]
    pub tcp_reconnection_enabled: bool,

    #[arg(long)]
    pub tcp_reconnection_max_retries: Option<u32>,

    #[arg(long, default_value = "1s")]
    pub tcp_reconnection_interval: String,

    #[arg(long, default_value = "5s")]
    pub tcp_reconnection_reestablish_after: String,

    #[arg(long, default_value = "5s")]
    pub tcp_heartbeat_interval: String,

    #[arg(long, default_value = "127.0.0.1:8090")]
    pub tcp_server_address: String,

    #[arg(long, default_value = "false")]
    pub tcp_tls_enabled: bool,

    #[arg(long, default_value = "localhost")]
    pub tcp_tls_domain: String,

    #[arg(long, default_value = "127.0.0.1:0")]
    pub quick_client_address: String,

    #[arg(long, default_value = "127.0.0.1:8080")]
    pub quick_server_address: String,

    #[arg(long, default_value = "localhost")]
    pub quick_server_name: String,

    #[arg(long, default_value = "true")]
    pub quick_reconnection_enabled: bool,

    #[arg(long)]
    pub quick_reconnection_max_retries: Option<u32>,

    #[arg(long, default_value = "1s")]
    pub quick_reconnection_interval: String,

    #[arg(long, default_value = "5s")]
    pub quick_reconnection_reestablish_after: String,

    #[arg(long, default_value = "10000")]
    pub quick_max_concurrent_bidi_streams: u64,

    #[arg(long, default_value = "100000")]
    pub quick_datagram_send_buffer_size: u64,

    #[arg(long, default_value = "1200")]
    pub quick_initial_mtu: u16,

    #[arg(long, default_value = "100000")]
    pub quick_send_window: u64,

    #[arg(long, default_value = "100000")]
    pub quick_receive_window: u64,

    #[arg(long, default_value = "1048576")]
    pub quick_response_buffer_size: u64,

    #[arg(long, default_value = "5000")]
    pub quick_keep_alive_interval: u64,

    #[arg(long, default_value = "10000")]
    pub quick_max_idle_timeout: u64,

    #[arg(long, default_value = "false")]
    pub quick_validate_certificate: bool,

    #[arg(long, default_value = "5s")]
    pub quick_heartbeat_interval: String,
}

impl Args {
    pub fn to_sdk_args(&self) -> iggy::args::Args {
        iggy::args::Args {
            transport: self.transport.clone(),
            encryption_key: self.encryption_key.clone(),
            http_api_url: self.http_api_url.clone(),
            http_retries: self.http_retries,
            username: self.username.clone(),
            password: self.password.clone(),
            tcp_server_address: self.tcp_server_address.clone(),
            tcp_reconnection_enabled: self.tcp_reconnection_enabled,
            tcp_reconnection_max_retries: self.tcp_reconnection_max_retries,
            tcp_reconnection_interval: self.tcp_reconnection_interval.clone(),
            tcp_reconnection_reestablish_after: self.tcp_reconnection_reestablish_after.clone(),
            tcp_heartbeat_interval: self.tcp_heartbeat_interval.clone(),
            tcp_tls_enabled: self.tcp_tls_enabled,
            tcp_tls_domain: self.tcp_tls_domain.clone(),
            tcp_tls_ca_file: None,
            quick_client_address: self.quick_client_address.clone(),
            quick_server_address: self.quick_server_address.clone(),
            quick_server_name: self.quick_server_name.clone(),
            quick_reconnection_enabled: self.quick_reconnection_enabled,
            quick_reconnection_max_retries: self.quick_reconnection_max_retries,
            quick_reconnection_reestablish_after: self.quick_reconnection_reestablish_after.clone(),
            quick_reconnection_interval: self.quick_reconnection_interval.clone(),
            quick_max_concurrent_bidi_streams: self.quick_max_concurrent_bidi_streams,
            quick_datagram_send_buffer_size: self.quick_datagram_send_buffer_size,
            quick_initial_mtu: self.quick_initial_mtu,
            quick_send_window: self.quick_send_window,
            quick_receive_window: self.quick_receive_window,
            quick_response_buffer_size: self.quick_response_buffer_size,
            quick_keep_alive_interval: self.quick_keep_alive_interval,
            quick_max_idle_timeout: self.quick_max_idle_timeout,
            quick_validate_certificate: self.quick_validate_certificate,
            quick_heartbeat_interval: self.quick_heartbeat_interval.clone(),
        }
    }

    pub fn get_interval(&self) -> Option<IggyDuration> {
        match self.interval.to_lowercase().as_str() {
            "" | "0" | "none" => None,
            x => Some(IggyDuration::from_str(x).expect("Invalid interval format")),
        }
    }
}
