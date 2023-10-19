use crate::args::Args;
use crate::client_factory::ClientFactory;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::tcp::client::TcpClient;
use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
pub struct TcpClientFactory {}

#[async_trait]
impl ClientFactory for TcpClientFactory {
    async fn create_client(&self, args: Arc<Args>) -> Box<dyn Client> {
        let client = TcpClient::new(&args.tcp_server_address).unwrap();
        client.connect().await.unwrap();
        Box::new(client)
    }
}

unsafe impl Send for TcpClientFactory {}
unsafe impl Sync for TcpClientFactory {}
