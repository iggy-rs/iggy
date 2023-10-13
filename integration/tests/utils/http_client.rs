use crate::utils::test_server::ClientFactory;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::http::client::HttpClient;
use iggy::http::config::HttpClientConfig;
use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
pub struct HttpClientFactory {}

#[async_trait]
impl ClientFactory for HttpClientFactory {
    async fn create_client(&self) -> Box<dyn Client> {
        let client = HttpClient::create(Arc::new(HttpClientConfig::default())).unwrap();
        Box::new(client)
    }
}

unsafe impl Send for HttpClientFactory {}
unsafe impl Sync for HttpClientFactory {}
