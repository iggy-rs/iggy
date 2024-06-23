use super::shard::IggyShard;
use crate::streaming::clients::client_manager::{Client, Transport};
use iggy::{error::IggyError, identifier::Identifier};
use std::net::SocketAddr;
use tracing::{error, info};

impl IggyShard {
    pub async fn add_client(&self, address: &SocketAddr, transport: Transport) -> u32 {
        // We should gossip that to every shard, so the client_manager is up to date everywhere.
        let mut client_manager = self.client_manager.borrow_mut();
        let client_id = client_manager.add_client(address, transport);
        info!("Added {transport} client with ID: {client_id} for IP address: {address}");
        self.metrics.increment_clients(1);
        client_id
    }

    pub async fn delete_client(&self, address: &SocketAddr) {
        let consumer_groups: Vec<(u32, u32, u32)>;
        let client_id;

        {
            let client_manager = self.client_manager.borrow();
            let client = client_manager.get_client_by_address(address);
            if client.is_err() {
                return;
            }

            let client = client.unwrap();
            client_id = client.client_id;

            consumer_groups = client
                .consumer_groups
                .iter()
                .map(|c| (c.stream_id, c.topic_id, c.group_id))
                .collect();
        }

        for (stream_id, topic_id, consumer_group_id) in consumer_groups.iter() {
            if let Err(error) = self
                .leave_consumer_group_by_client(
                    &Identifier::numeric(*stream_id).unwrap(),
                    &Identifier::numeric(*topic_id).unwrap(),
                    &Identifier::numeric(*consumer_group_id).unwrap(),
                    client_id,
                )
                .await
            {
                error!(
                    "Failed to leave consumer group with ID: {} by client with ID: {}. Error: {}",
                    consumer_group_id, client_id, error
                );
            }
        }

        let mut client_manager = self.client_manager.borrow_mut();
        let client = client_manager.delete_client(address);
        if client.is_none() {
            return;
        }

        self.metrics.decrement_clients(1);
        let client = client.unwrap();

        info!(
            "Deleted {} client with ID: {} for IP address: {}",
            client.transport, client.client_id, client.address
        );
    }

    pub async fn get_client(&self, client_id: u32) -> Result<Client, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        self.permissioner.borrow().get_client(user_id)?;
        let client_manager = self.client_manager.borrow();
        client_manager.get_client_by_id(client_id)
    }

    pub async fn get_clients(&self, client_id: u32) -> Result<Vec<Client>, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        self.permissioner.borrow().get_clients(user_id)?;
        let client_manager = self.client_manager.borrow();
        Ok(client_manager.get_clients())
    }
}
