use crate::client::SystemClient;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
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
    async fn get_stats(&self) -> Result<Stats, IggyError> {
        get_stats(self, &GetStats {}).await
    }

    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
        get_me(self, &GetMe {}).await
    }

    async fn get_client(&self, client_id: u32) -> Result<ClientInfoDetails, IggyError> {
        get_client(self, &GetClient { client_id }).await
    }

    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
        get_clients(self, &GetClients {}).await
    }

    async fn ping(&self) -> Result<(), IggyError> {
        ping(self, &Ping {}).await
    }
}

async fn get_stats<T: HttpTransport>(
    transport: &T,
    _command: &GetStats,
) -> Result<Stats, IggyError> {
    let response = transport.get(STATS).await?;
    let stats = response.json().await?;
    Ok(stats)
}

async fn get_me<T: HttpTransport>(_: &T, _command: &GetMe) -> Result<ClientInfoDetails, IggyError> {
    Err(IggyError::FeatureUnavailable)
}

async fn get_client<T: HttpTransport>(
    transport: &T,
    command: &GetClient,
) -> Result<ClientInfoDetails, IggyError> {
    let path = format!("{}/{}", CLIENTS, command.client_id);
    let response = transport.get(&path).await?;
    let client = response.json().await?;
    Ok(client)
}

async fn get_clients<T: HttpTransport>(
    transport: &T,
    _command: &GetClients,
) -> Result<Vec<ClientInfo>, IggyError> {
    let response = transport.get(CLIENTS).await?;
    let clients = response.json().await?;
    Ok(clients)
}

async fn ping<T: HttpTransport>(transport: &T, _command: &Ping) -> Result<(), IggyError> {
    transport.get(PING).await?;
    Ok(())
}
