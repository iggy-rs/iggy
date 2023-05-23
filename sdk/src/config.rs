#[derive(Debug, Clone)]
pub struct Config {
    pub client_address: String,
    pub server_address: String,
    pub server_name: String,
    pub response_buffer_size: u64,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            client_address: "127.0.0.1:0".to_string(),
            server_address: "127.0.0.1:8080".to_string(),
            server_name: "localhost".to_string(),
            response_buffer_size: 1024 * 1024 * 10,
        }
    }
}
