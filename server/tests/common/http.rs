use crate::common::ClientFactory;
use async_trait::async_trait;
use sdk::client::Client;
use sdk::http::client::HttpClient;

#[derive(Debug, Copy, Clone)]
pub struct HttpClientFactory {}

#[async_trait]
impl ClientFactory for HttpClientFactory {
    async fn create_client(&self) -> Box<dyn Client> {
        let api_url = "http://localhost:3000";
        let client = HttpClient::new(api_url).unwrap();
        Box::new(client)
    }
}

unsafe impl Send for HttpClientFactory {}
unsafe impl Sync for HttpClientFactory {}
