use crate::streaming::utils::hash;
use iggy::error::IggyError;
use iggy::locking::IggySharedMut;
use iggy::locking::IggySharedMutFn;
use iggy::models::user_info::UserId;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;

#[derive(Debug, Default)]
pub struct ClientManager {
    clients: HashMap<u32, IggySharedMut<Client>>,
}

#[derive(Debug)]
pub struct Client {
    pub client_id: u32,
    pub user_id: Option<u32>,
    pub address: SocketAddr,
    pub transport: Transport,
    pub consumer_groups: Vec<ConsumerGroup>,
}

#[derive(Debug)]
pub struct ConsumerGroup {
    pub stream_id: u32,
    pub topic_id: u32,
    pub consumer_group_id: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum Transport {
    Tcp,
    Quic,
}

impl Display for Transport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Transport::Tcp => write!(f, "TCP"),
            Transport::Quic => write!(f, "QUIC"),
        }
    }
}

impl ClientManager {
    pub fn add_client(&mut self, address: &SocketAddr, transport: Transport) -> u32 {
        let id = hash::calculate_32(address.to_string().as_bytes());
        let client = Client {
            client_id: id,
            user_id: None,
            address: *address,
            transport,
            consumer_groups: Vec::new(),
        };
        self.clients
            .insert(client.client_id, IggySharedMut::new(client));
        id
    }

    pub async fn set_user_id(&mut self, client_id: u32, user_id: UserId) -> Result<(), IggyError> {
        let client = self.clients.get(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }

        let mut client = client.unwrap().write().await;
        client.user_id = Some(user_id);
        Ok(())
    }

    pub async fn clear_user_id(&mut self, client_id: u32) -> Result<(), IggyError> {
        let client = self.clients.get(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }

        let mut client = client.unwrap().write().await;
        client.user_id = None;
        Ok(())
    }

    pub fn get_client_by_address(
        &self,
        address: &SocketAddr,
    ) -> Result<IggySharedMut<Client>, IggyError> {
        let id = hash::calculate_32(address.to_string().as_bytes());
        self.get_client_by_id(id)
    }

    pub fn get_client_by_id(&self, client_id: u32) -> Result<IggySharedMut<Client>, IggyError> {
        let client = self.clients.get(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }

        Ok(client.unwrap().clone())
    }

    pub fn get_clients(&self) -> Vec<IggySharedMut<Client>> {
        self.clients.values().cloned().collect()
    }

    pub async fn delete_clients_for_user(&mut self, user_id: UserId) -> Result<(), IggyError> {
        let mut clients_to_remove = Vec::new();
        for client in self.clients.values() {
            let client = client.read().await;
            if let Some(client_user_id) = client.user_id {
                if client_user_id == user_id {
                    clients_to_remove.push(client.client_id);
                }
            }
        }

        for client_id in clients_to_remove {
            self.clients.remove(&client_id);
        }

        Ok(())
    }

    pub fn delete_client(&mut self, address: &SocketAddr) -> Option<IggySharedMut<Client>> {
        let id = hash::calculate_32(address.to_string().as_bytes());
        self.clients.remove(&id)
    }

    pub async fn join_consumer_group(
        &self,
        client_id: u32,
        stream_id: u32,
        topic_id: u32,
        consumer_group_id: u32,
    ) -> Result<(), IggyError> {
        let client = self.clients.get(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }

        let mut client = client.unwrap().write().await;
        if client.consumer_groups.iter().any(|consumer_group| {
            consumer_group.consumer_group_id == consumer_group_id
                && consumer_group.topic_id == topic_id
                && consumer_group.stream_id == stream_id
        }) {
            return Ok(());
        }

        client.consumer_groups.push(ConsumerGroup {
            stream_id,
            topic_id,
            consumer_group_id,
        });
        Ok(())
    }

    pub async fn leave_consumer_group(
        &self,
        client_id: u32,
        stream_id: u32,
        topic_id: u32,
        consumer_group_id: u32,
    ) -> Result<(), IggyError> {
        let client = self.clients.get(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }
        let mut client = client.unwrap().write().await;
        for (index, consumer_group) in client.consumer_groups.iter().enumerate() {
            if consumer_group.stream_id == stream_id
                && consumer_group.topic_id == topic_id
                && consumer_group.consumer_group_id == consumer_group_id
            {
                client.consumer_groups.remove(index);
                return Ok(());
            }
        }
        Ok(())
    }

    pub async fn delete_consumer_groups_for_stream(&self, stream_id: u32) {
        for client in self.clients.values() {
            let mut client = client.write().await;
            let indexes_to_remove = client
                .consumer_groups
                .iter()
                .enumerate()
                .filter_map(|(index, consumer_group)| {
                    if consumer_group.stream_id == stream_id {
                        Some(index)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            for index in indexes_to_remove {
                client.consumer_groups.remove(index);
            }
        }
    }

    pub async fn delete_consumer_groups_for_topic(&self, stream_id: u32, topic_id: u32) {
        for client in self.clients.values() {
            let mut client = client.write().await;
            let indexes_to_remove = client
                .consumer_groups
                .iter()
                .enumerate()
                .filter_map(|(index, consumer_group)| {
                    if consumer_group.stream_id == stream_id && consumer_group.topic_id == topic_id
                    {
                        Some(index)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            for index in indexes_to_remove {
                client.consumer_groups.remove(index);
            }
        }
    }
}
