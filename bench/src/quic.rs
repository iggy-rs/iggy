use crate::args::Args;
use crate::client_factory::ClientFactory;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::quic::client::QuicClient;
use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
pub struct QuicClientFactory {}

#[async_trait]
impl ClientFactory for QuicClientFactory {
    async fn create_client(&self, args: Arc<Args>) -> Box<dyn Client> {
        let client = QuicClient::new(
            &args.quic_client_address,
            &args.quic_server_address,
            &args.quic_server_name,
            args.quic_validate_certificate,
        )
        .unwrap();
        client.connect().await.unwrap();
        Box::new(client)
    }
}

unsafe impl Send for QuicClientFactory {}
unsafe impl Sync for QuicClientFactory {}
