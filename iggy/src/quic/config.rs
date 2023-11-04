/// Configuration for the QUIC client.
#[derive(Debug, Clone)]
pub struct QuicClientConfig {
    /// The address to bind the QUIC client to.
    pub client_address: String,
    /// The address of the QUIC server to connect to.
    pub server_address: String,
    /// The server name to use.
    pub server_name: String,
    /// The number of reconnection retries.
    pub reconnection_retries: u32,
    /// The interval between reconnection retries.
    pub reconnection_interval: u64,
    /// The size of the response buffer.
    pub response_buffer_size: u64,
    /// The maximum number of concurrent bidirectional streams.
    pub max_concurrent_bidi_streams: u64,
    /// The size of the datagram send buffer.
    pub datagram_send_buffer_size: u64,
    /// The initial MTU.
    pub initial_mtu: u16,
    /// The send window.
    pub send_window: u64,
    /// The receive window.
    pub receive_window: u64,
    /// The keep alive interval.
    pub keep_alive_interval: u64,
    /// The maximum idle timeout.
    pub max_idle_timeout: u64,
    /// Whether to validate the server certificate.
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
