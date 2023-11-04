/// Configuration for the HTTP client.
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// The URL of the Iggy API.
    pub api_url: String,
    /// The number of retries to perform on transient errors.
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
