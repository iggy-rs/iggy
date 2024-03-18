use crate::test_server::{ClientFactory, ClientFactoryV2};
use async_trait::async_trait;
use iggy::client::Client;
use iggy::client_v2::ClientV2;
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
        let client = TcpClient::create(Arc::new(config)).unwrap_or_else(|e| {
            panic!(
                "Failed to create TcpClient, iggy-server has address {}, error: {:?}",
                self.server_addr, e
            )
        });
        iggy::client::Client::connect(&client)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to connect to iggy-server at {}, error: {:?}",
                    self.server_addr, e
                )
            });
        Box::new(client)
    }
}

#[async_trait]
impl ClientFactoryV2 for TcpClientFactory {
    async fn create_client(&self) -> Box<dyn ClientV2> {
        let config = TcpClientConfig {
            server_address: self.server_addr.clone(),
            ..TcpClientConfig::default()
        };
        let client = TcpClient::create(Arc::new(config)).unwrap_or_else(|e| {
            panic!(
                "Failed to create TcpClient, iggy-server has address {}, error: {:?}",
                self.server_addr, e
            )
        });
        iggy::client_v2::ClientV2::connect(&client)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to connect to iggy-server at {}, error: {:?}",
                    self.server_addr, e
                )
            });
        Box::new(client)
    }
}

unsafe impl Send for TcpClientFactory {}
unsafe impl Sync for TcpClientFactory {}
