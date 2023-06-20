use crate::common::ClientFactory;
use async_trait::async_trait;
use sdk::client::Client;
use sdk::http::client::HttpClient;
use sdk::http::config::HttpClientConfig;
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
