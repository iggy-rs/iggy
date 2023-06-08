use crate::common::ClientFactory;
use async_trait::async_trait;
use sdk::client::Client;
use sdk::tcp::client::TcpClient;

#[derive(Debug, Copy, Clone)]
pub struct TcpClientFactory {}

#[async_trait]
impl ClientFactory for TcpClientFactory {
    async fn create_client(&self) -> Box<dyn Client> {
        let server_address = "127.0.0.1:8090";
        let mut client = TcpClient::new(server_address).unwrap();
        client.connect().await.unwrap();
        Box::new(client)
    }
}

unsafe impl Send for TcpClientFactory {}
unsafe impl Sync for TcpClientFactory {}
