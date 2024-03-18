use crate::test_server::{ClientFactory, ClientFactoryV2};
use async_trait::async_trait;
use iggy::client::Client;
use iggy::client_v2::ClientV2;
use iggy::quic::client::QuicClient;
use iggy::quic::config::QuicClientConfig;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct QuicClientFactory {
    pub server_addr: String,
}

#[async_trait]
impl ClientFactory for QuicClientFactory {
    async fn create_client(&self) -> Box<dyn Client> {
        let config = QuicClientConfig {
            server_address: self.server_addr.clone(),
            ..QuicClientConfig::default()
        };
        let client = QuicClient::create(Arc::new(config)).unwrap();
        iggy::client::Client::connect(&client).await.unwrap();
        Box::new(client)
    }
}

#[async_trait]
impl ClientFactoryV2 for QuicClientFactory {
    async fn create_client(&self) -> Box<dyn ClientV2> {
        let config = QuicClientConfig {
            server_address: self.server_addr.clone(),
            ..QuicClientConfig::default()
        };
        let client = QuicClient::create(Arc::new(config)).unwrap();
        iggy::client_v2::ClientV2::connect(&client).await.unwrap();
        Box::new(client)
    }
}

unsafe impl Send for QuicClientFactory {}
unsafe impl Sync for QuicClientFactory {}
