use crate::client::SystemClient;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::stats::Stats;
use crate::system::get_client::GetClient;
use crate::system::get_clients::GetClients;
use crate::system::get_me::GetMe;
use crate::system::get_stats::GetStats;
use crate::system::ping::Ping;
use async_trait::async_trait;

const PING: &str = "/ping";
const CLIENTS: &str = "/clients";
const STATS: &str = "/stats";

#[async_trait]
impl SystemClient for HttpClient {
    async fn get_stats(&self, _command: &GetStats) -> Result<Stats, Error> {
        let response = self.get(STATS).await?;
        let stats = response.json().await?;
        Ok(stats)
    }

    async fn get_me(&self, _command: &GetMe) -> Result<ClientInfoDetails, Error> {
        Err(Error::FeatureUnavailable)
    }

    async fn get_client(&self, command: &GetClient) -> Result<ClientInfoDetails, Error> {
        let path = format!("{}/{}", CLIENTS, command.client_id);
        let response = self.get(&path).await?;
        let client = response.json().await?;
        Ok(client)
    }

    async fn get_clients(&self, _command: &GetClients) -> Result<Vec<ClientInfo>, Error> {
        let response = self.get(CLIENTS).await?;
        let clients = response.json().await?;
        Ok(clients)
    }

    async fn ping(&self, _command: &Ping) -> Result<(), Error> {
        self.get(PING).await?;
        Ok(())
    }
}
