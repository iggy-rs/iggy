#[derive(Debug, Clone)]
pub struct Config {
    pub client_address: String,
    pub server_address: String,
    pub server_name: String,
    pub response_buffer_size: u64,
}
