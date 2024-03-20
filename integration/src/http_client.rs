use crate::test_server::{ClientFactory, ClientFactoryV2};
use async_trait::async_trait;
use iggy::client::Client;
use iggy::client_v2::ClientV2;
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

#[async_trait]
impl ClientFactoryV2 for HttpClientFactory {
    async fn create_client(&self) -> Box<dyn ClientV2> {
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
