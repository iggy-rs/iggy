use crate::binary::binary_client::{BinaryClient, BinaryClientNext};
use crate::binary::{fail_if_not_authenticated, mapper, BinaryTransport};
use crate::bytes_serializable::BytesSerializable;
use crate::client::SystemClient;
use crate::command::{GET_CLIENTS_CODE, GET_CLIENT_CODE, GET_ME_CODE, GET_STATS_CODE, PING_CODE};
use crate::error::IggyError;
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::stats::Stats;
use crate::next_client::SystemClientNext;
use crate::system::get_client::GetClient;
use crate::system::get_clients::GetClients;
use crate::system::get_me::GetMe;
use crate::system::get_stats::GetStats;
use crate::system::ping::Ping;

#[async_trait::async_trait]
impl<B: BinaryClient> SystemClient for B {
    async fn get_stats(&self, command: &GetStats) -> Result<Stats, IggyError> {
        get_stats(self, command).await
    }

    async fn get_me(&self, command: &GetMe) -> Result<ClientInfoDetails, IggyError> {
        get_me(self, command).await
    }

    async fn get_client(&self, command: &GetClient) -> Result<ClientInfoDetails, IggyError> {
        get_client(self, command).await
    }

    async fn get_clients(&self, command: &GetClients) -> Result<Vec<ClientInfo>, IggyError> {
        get_clients(self, command).await
    }

    async fn ping(&self, command: &Ping) -> Result<(), IggyError> {
        ping(self, command).await
    }
}

#[async_trait::async_trait]
impl<B: BinaryClientNext> SystemClientNext for B {
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

async fn get_stats<T: BinaryTransport>(
    transport: &T,
    command: &GetStats,
) -> Result<Stats, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_STATS_CODE, command.as_bytes())
        .await?;
    mapper::map_stats(response)
}

async fn get_me<T: BinaryTransport>(
    transport: &T,
    command: &GetMe,
) -> Result<ClientInfoDetails, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_ME_CODE, command.as_bytes())
        .await?;
    mapper::map_client(response)
}

async fn get_client<T: BinaryTransport>(
    transport: &T,
    command: &GetClient,
) -> Result<ClientInfoDetails, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_CLIENT_CODE, command.as_bytes())
        .await?;
    mapper::map_client(response)
}

async fn get_clients<T: BinaryTransport>(
    transport: &T,
    command: &GetClients,
) -> Result<Vec<ClientInfo>, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_CLIENTS_CODE, command.as_bytes())
        .await?;
    mapper::map_clients(response)
}

async fn ping<T: BinaryTransport>(transport: &T, command: &Ping) -> Result<(), IggyError> {
    transport
        .send_with_response(PING_CODE, command.as_bytes())
        .await?;
    Ok(())
}
