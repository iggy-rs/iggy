use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct QuicConfig {
    pub enabled: bool,
    pub address: String,
    pub max_concurrent_bidi_streams: u64,
    pub datagram_send_buffer_size: usize,
    pub initial_mtu: u16,
    pub send_window: u64,
    pub receive_window: u64,
    pub keep_alive_interval: u64,
    pub max_idle_timeout: u64,
    pub certificate: QuicCertificateConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct QuicCertificateConfig {
    pub self_signed: bool,
    pub cert_file: String,
    pub key_file: String,
}
