use crate::test_server::ClientFactory;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::tcp::client::TcpClient;
use iggy::tcp::config::TcpClientConfig;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TcpClientFactory {
    pub server_addr: String,
}

#[async_trait]
impl ClientFactory for TcpClientFactory {
    async fn create_client(&self) -> Box<dyn Client> {
        let config = TcpClientConfig {
            server_address: self.server_addr.clone(),
            ..TcpClientConfig::default()
        };
        let client = TcpClient::create(Arc::new(config)).unwrap();
        client.connect().await.unwrap();
        Box::new(client)
    }
}

unsafe impl Send for TcpClientFactory {}
unsafe impl Sync for TcpClientFactory {}
