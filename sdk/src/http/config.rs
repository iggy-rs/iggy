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

/// The builder for the `HttpClientConfig` configuration.
/// Allows configuring the HTTP client with custom settings or using defaults:
/// - `api_url`: Default is "http://127.0.0.1:3000"
/// - `retries`: Default is 3.
#[derive(Debug, Default)]
pub struct HttpClientConfigBuilder {
    config: HttpClientConfig,
}

impl HttpClientConfigBuilder {
    /// Create a new `HttpClientConfigBuilder` with default settings.
    pub fn new() -> Self {
        HttpClientConfigBuilder::default()
    }

    /// Sets the API URL for the HTTP client.
    pub fn with_api_url(mut self, url: String) -> Self {
        self.config.api_url = url;
        self
    }

    /// Sets the number of retries for the HTTP client.
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.config.retries = retries;
        self
    }

    /// Builds the `HttpClientConfig` instance.
    pub fn build(self) -> HttpClientConfig {
        self.config
    }
}
