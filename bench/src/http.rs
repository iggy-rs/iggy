use crate::args::Args;
use crate::client_factory::ClientFactory;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::http::client::HttpClient;
use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
pub struct HttpClientFactory {}

#[async_trait]
impl ClientFactory for HttpClientFactory {
    async fn create_client(&self, args: Arc<Args>) -> Box<dyn Client> {
        let client = HttpClient::new(&args.http_api_url).unwrap();
        Box::new(client)
    }
}

unsafe impl Send for HttpClientFactory {}
unsafe impl Sync for HttpClientFactory {}
