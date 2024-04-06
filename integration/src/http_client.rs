use crate::test_server::ClientFactory;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::http::client::HttpClient;
use iggy::http::config::HttpClientConfig;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct HttpClientFactory {
    pub server_addr: String,
}

#[async_trait]
impl ClientFactory for HttpClientFactory {
    async fn create_client(&self) -> Box<dyn Client> {
        let config = HttpClientConfig {
            api_url: format!("http://{}", self.server_addr.clone()),
            ..HttpClientConfig::default()
        };
        let client = HttpClient::create(Arc::new(config)).unwrap();
        Box::new(client)
    }
}

unsafe impl Send for HttpClientFactory {}
unsafe impl Sync for HttpClientFactory {}
