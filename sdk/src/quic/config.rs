#[derive(Debug, Clone)]
pub struct Config {
    pub client_address: String,
    pub server_address: String,
    pub server_name: String,
    pub response_buffer_size: u64,
    pub max_concurrent_bidi_streams: u64,
    pub datagram_send_buffer_size: usize,
    pub initial_mtu: u16,
    pub send_window: u64,
    pub receive_window: u64,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            client_address: "127.0.0.1:0".to_string(),
            server_address: "127.0.0.1:8080".to_string(),
            server_name: "localhost".to_string(),
            response_buffer_size: 1024 * 1024 * 10,
            max_concurrent_bidi_streams: 10000,
            datagram_send_buffer_size: 100000,
            initial_mtu: 1200,
            send_window: 100000,
            receive_window: 100000,
        }
    }
}
