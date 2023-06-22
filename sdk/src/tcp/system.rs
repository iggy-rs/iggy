use crate::binary;
use crate::client::SystemClient;
use crate::error::Error;
use crate::models::client_info::ClientInfo;
use crate::system::get_clients::GetClients;
use crate::system::kill::Kill;
use crate::system::ping::Ping;
use crate::tcp::client::TcpClient;
use async_trait::async_trait;

#[async_trait]
impl SystemClient for TcpClient {
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
