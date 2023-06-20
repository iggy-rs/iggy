use crate::common::ClientFactory;
use async_trait::async_trait;
use sdk::client::Client;
use sdk::quic::client::QuicClient;
use sdk::quic::config::QuicClientConfig;
use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
pub struct QuicClientFactory {}

#[async_trait]
impl ClientFactory for QuicClientFactory {
    async fn create_client(&self) -> Box<dyn Client> {
        let mut client = QuicClient::create(Arc::new(QuicClientConfig::default())).unwrap();
        client.connect().await.unwrap();
        Box::new(client)
    }
}

unsafe impl Send for QuicClientFactory {}
unsafe impl Sync for QuicClientFactory {}
