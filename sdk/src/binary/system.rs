use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::bytes_serializable::BytesSerializable;
use crate::client::SystemClient;
use crate::command::{GET_CLIENTS_CODE, GET_CLIENT_CODE, GET_ME_CODE, GET_STATS_CODE, PING_CODE};
use crate::error::IggyError;
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::stats::Stats;
use crate::system::get_client::GetClient;
use crate::system::get_clients::GetClients;
use crate::system::get_me::GetMe;
use crate::system::get_stats::GetStats;
use crate::system::ping::Ping;

#[async_trait::async_trait]
impl<B: BinaryClient> SystemClient for B {
    async fn get_stats(&self) -> Result<Stats, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(GET_STATS_CODE, GetStats {}.to_bytes())
            .await?;
        mapper::map_stats(response)
    }

    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(GET_ME_CODE, GetMe {}.to_bytes())
            .await?;
        mapper::map_client(response)
    }

    async fn get_client(&self, client_id: u32) -> Result<ClientInfoDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(GET_CLIENT_CODE, GetClient { client_id }.to_bytes())
            .await?;
        mapper::map_client(response)
    }

    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(GET_CLIENTS_CODE, GetClients {}.to_bytes())
            .await?;
        mapper::map_clients(response)
    }

    async fn ping(&self) -> Result<(), IggyError> {
        self.send_with_response(PING_CODE, Ping {}.to_bytes())
            .await?;
        Ok(())
    }
}
