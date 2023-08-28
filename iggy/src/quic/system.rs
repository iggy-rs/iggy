use crate::binary;
use crate::client::SystemClient;
use crate::error::Error;
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::stats::Stats;
use crate::quic::client::QuicClient;
use crate::system::get_client::GetClient;
use crate::system::get_clients::GetClients;
use crate::system::get_me::GetMe;
use crate::system::get_stats::GetStats;
use crate::system::ping::Ping;
use async_trait::async_trait;

#[async_trait]
impl SystemClient for QuicClient {
    async fn get_stats(&self, command: &GetStats) -> Result<Stats, Error> {
        binary::system::get_stats(self, command).await
    }

    async fn get_me(&self, command: &GetMe) -> Result<ClientInfoDetails, Error> {
        binary::system::get_me(self, command).await
    }

    async fn get_client(&self, command: &GetClient) -> Result<ClientInfoDetails, Error> {
        binary::system::get_client(self, command).await
    }

    async fn get_clients(&self, command: &GetClients) -> Result<Vec<ClientInfo>, Error> {
        binary::system::get_clients(self, command).await
    }

    async fn ping(&self, command: &Ping) -> Result<(), Error> {
        binary::system::ping(self, command).await
    }
}
