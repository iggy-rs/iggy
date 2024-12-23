use crate::client::SystemClient;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::snapshot::Snapshot;
use crate::models::stats::Stats;
use crate::snapshot::{SnapshotCompression, SystemSnapshotType};
use crate::system::get_snapshot::GetSnapshot;
use crate::utils::duration::IggyDuration;
use async_trait::async_trait;

const PING: &str = "/ping";
const CLIENTS: &str = "/clients";
const STATS: &str = "/stats";
const SNAPSHOT: &str = "/snapshot";

#[async_trait]
impl SystemClient for HttpClient {
    async fn get_stats(&self) -> Result<Stats, IggyError> {
        let response = self.get(STATS).await?;
        let stats = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(stats)
    }

    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
        Err(IggyError::FeatureUnavailable)
    }

    async fn get_client(&self, client_id: u32) -> Result<Option<ClientInfoDetails>, IggyError> {
        let response = self.get(&format!("{}/{}", CLIENTS, client_id)).await?;
        if response.status() == 404 {
            return Ok(None);
        }

        let client = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(Some(client))
    }

    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
        let response = self.get(CLIENTS).await?;
        let clients = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(clients)
    }

    async fn ping(&self) -> Result<(), IggyError> {
        self.get(PING).await?;
        Ok(())
    }

    async fn heartbeat_interval(&self) -> IggyDuration {
        self.heartbeat_interval
    }

    async fn snapshot(
        &self,
        compression: SnapshotCompression,
        snapshot_types: Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError> {
        let response = self
            .post(
                SNAPSHOT,
                &GetSnapshot {
                    compression,
                    snapshot_types,
                },
            )
            .await?;
        let file = response
            .bytes()
            .await
            .map_err(|_| IggyError::InvalidBytesResponse)?;
        let snapshot = Snapshot::new(file.to_vec());
        Ok(snapshot)
    }
}
