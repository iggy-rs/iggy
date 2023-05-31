use crate::common::ClientFactory;
use async_trait::async_trait;
use sdk::client::Client;
use sdk::quic::client::QuicBaseClient;
use sdk::quic::config::Config;

#[derive(Debug, Copy, Clone)]
pub struct QuicClientFactory {}

#[async_trait]
impl ClientFactory for QuicClientFactory {
    async fn create_client(&self) -> Box<dyn Client> {
        let client = QuicBaseClient::create(Config::default()).unwrap();
        let client = client.connect().await.unwrap();
        Box::new(client)
    }
}

unsafe impl Send for QuicClientFactory {}
unsafe impl Sync for QuicClientFactory {}
