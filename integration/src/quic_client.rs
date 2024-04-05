use crate::test_server::ClientFactory;
use async_trait::async_trait;
use iggy::client::Client;
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

unsafe impl Send for QuicClientFactory {}
unsafe impl Sync for QuicClientFactory {}
