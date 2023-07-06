use crate::binary;
use crate::client::SystemClient;
use crate::error::Error;
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::quic::client::QuicClient;
use crate::system::get_client::GetClient;
use crate::system::get_clients::GetClients;
use crate::system::kill::Kill;
use crate::system::ping::Ping;
use async_trait::async_trait;

#[async_trait]
impl SystemClient for QuicClient {
    async fn get_client(&self, command: &GetClient) -> Result<ClientInfoDetails, Error> {
        binary::system::get_client(self, command).await
    }

    async fn get_clients(&self, command: &GetClients) -> Result<Vec<ClientInfo>, Error> {
        binary::system::get_clients(self, command).await
    }

    async fn ping(&self, command: &Ping) -> Result<(), Error> {
        binary::system::ping(self, command).await
    }

    async fn kill(&self, command: &Kill) -> Result<(), Error> {
        binary::system::kill(self, command).await
    }
}
