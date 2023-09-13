#[derive(Debug, Clone)]
pub struct QuicClientConfig {
    pub client_address: String,
    pub server_address: String,
    pub server_name: String,
    pub reconnection_retries: u32,
    pub reconnection_interval: u64,
    pub response_buffer_size: u64,
    pub max_concurrent_bidi_streams: u64,
    pub datagram_send_buffer_size: u64,
    pub initial_mtu: u16,
    pub send_window: u64,
    pub receive_window: u64,
    pub keep_alive_interval: u64,
    pub max_idle_timeout: u64,
    pub validate_certificate: bool,
}

impl Default for QuicClientConfig {
    fn default() -> QuicClientConfig {
        QuicClientConfig {
            client_address: "127.0.0.1:0".to_string(),
            server_address: "127.0.0.1:8080".to_string(),
            server_name: "localhost".to_string(),
            reconnection_retries: 3,
            reconnection_interval: 1000,
            response_buffer_size: 1024 * 1024 * 10,
            max_concurrent_bidi_streams: 10000,
            datagram_send_buffer_size: 100_000,
            initial_mtu: 1200,
            send_window: 100_000,
            receive_window: 100_000,
            keep_alive_interval: 5000,
            max_idle_timeout: 10000,
            validate_certificate: false,
        }
    }
}
