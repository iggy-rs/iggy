use crate::server_tests::common::ClientFactory;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::tcp::client::TcpClient;
use iggy::tcp::config::TcpClientConfig;
use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
pub struct TcpClientFactory {}

#[async_trait]
impl ClientFactory for TcpClientFactory {
    async fn create_client(&self) -> Box<dyn Client> {
        let mut client = TcpClient::create(Arc::new(TcpClientConfig::default())).unwrap();
        client.connect().await.unwrap();
        Box::new(client)
    }
}

unsafe impl Send for TcpClientFactory {}
unsafe impl Sync for TcpClientFactory {}
