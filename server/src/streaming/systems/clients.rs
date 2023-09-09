use crate::streaming::clients::client_manager::{Client, Transport};
use crate::streaming::systems::system::System;
use iggy::error::Error;
use iggy::identifier::Identifier;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

impl System {
    pub async fn add_client(&self, address: &SocketAddr, transport: Transport) -> u32 {
        let mut client_manager = self.client_manager.write().await;
        let client_id = client_manager.add_client(address, transport);
        info!("Added {transport} client with ID: {client_id} for address: {address}");
        client_id
    }

    pub async fn delete_client(&self, address: &SocketAddr) {
        let consumer_groups: Vec<(u32, u32, u32)>;
        let client_id;

        {
            let client_manager = self.client_manager.read().await;
            let client = client_manager.get_client_by_address(address);
            if client.is_err() {
                return;
            }

            let client = client.unwrap();
            let client = client.read().await;
            client_id = client.id;

            consumer_groups = client
                .consumer_groups
                .iter()
                .map(|c| (c.stream_id, c.topic_id, c.consumer_group_id))
                .collect();
        }

        for (stream_id, topic_id, consumer_group_id) in consumer_groups.iter() {
            if let Err(error) = self
                .leave_consumer_group(
                    client_id,
                    &Identifier::numeric(*stream_id).unwrap(),
                    &Identifier::numeric(*topic_id).unwrap(),
                    *consumer_group_id,
                )
                .await
            {
                error!(
                    "Failed to leave consumer group with ID: {} by client with ID: {}. Error: {}",
                    consumer_group_id, client_id, error
                );
            }
        }

        {
            let mut client_manager = self.client_manager.write().await;
            let client = client_manager.delete_client(address);
            if client.is_none() {
                return;
            }

            let client = client.unwrap();
            let client = client.read().await;

            info!(
                "Deleted {} client with ID: {} for address: {}",
                client.transport, client.id, client.address
            );
        }
    }

    pub async fn get_client(&self, client_id: u32) -> Result<Arc<RwLock<Client>>, Error> {
        let client_manager = self.client_manager.read().await;
        client_manager.get_client_by_id(client_id)
    }

    pub async fn get_clients(&self) -> Vec<Arc<RwLock<Client>>> {
        let client_manager = self.client_manager.read().await;
        client_manager.get_clients()
    }
}
