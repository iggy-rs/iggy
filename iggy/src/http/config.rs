#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    pub api_url: String,
    pub retries: u32,
}

impl Default for HttpClientConfig {
    fn default() -> HttpClientConfig {
        HttpClientConfig {
            api_url: "http://127.0.0.1:3000".to_string(),
            retries: 3,
        }
    }
}
