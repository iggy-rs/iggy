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
    clients: HashMap<u32, Client>,
}

#[derive(Debug, Clone)]
pub struct Client {
    pub client_id: u32,
    pub user_id: Option<u32>,
    pub address: SocketAddr,
    pub transport: Transport,
    pub consumer_groups: Vec<ConsumerGroup>,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub stream_id: u32,
    pub topic_id: u32,
    pub group_id: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum Transport {
    Tcp,
}

impl Display for Transport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Transport::Tcp => write!(f, "TCP"),
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
        self.clients.insert(client.client_id, client);
        id
    }

    pub async fn set_user_id(&mut self, client_id: u32, user_id: UserId) -> Result<(), IggyError> {
        let client = self.clients.get_mut(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }

        let client = client.unwrap();
        client.user_id = Some(user_id);
        Ok(())
    }

    pub async fn clear_user_id(&mut self, client_id: u32) -> Result<(), IggyError> {
        let client = self.clients.get_mut(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }

        let client = client.unwrap();
        client.user_id = None;
        Ok(())
    }

    pub fn get_client_by_address(&self, address: &SocketAddr) -> Result<Client, IggyError> {
        let id = hash::calculate_32(address.to_string().as_bytes());
        self.get_client_by_id(id)
    }

    pub fn get_client_by_id(&self, client_id: u32) -> Result<Client, IggyError> {
        let client = self.clients.get(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }

        Ok(client.unwrap().clone())
    }

    pub fn get_clients(&self) -> Vec<Client> {
        self.clients.values().cloned().collect()
    }

    pub async fn delete_clients_for_user(&mut self, user_id: UserId) -> Result<(), IggyError> {
        let mut clients_to_remove = Vec::new();
        for client in self.clients.values() {
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

    pub fn delete_client(&mut self, address: &SocketAddr) -> Option<Client> {
        let id = hash::calculate_32(address.to_string().as_bytes());
        self.clients.remove(&id)
    }

    pub async fn join_consumer_group(
        &mut self,
        client_id: u32,
        stream_id: u32,
        topic_id: u32,
        group_id: u32,
    ) -> Result<(), IggyError> {
        let client = self.clients.get_mut(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }

        let client = client.unwrap();
        if client.consumer_groups.iter().any(|consumer_group| {
            consumer_group.group_id == group_id
                && consumer_group.topic_id == topic_id
                && consumer_group.stream_id == stream_id
        }) {
            return Ok(());
        }

        client.consumer_groups.push(ConsumerGroup {
            stream_id,
            topic_id,
            group_id,
        });
        Ok(())
    }

    pub async fn leave_consumer_group(
        &mut self,
        client_id: u32,
        stream_id: u32,
        topic_id: u32,
        consumer_group_id: u32,
    ) -> Result<(), IggyError> {
        let client = self.clients.get_mut(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }
        let mut client = client.unwrap();
        for (index, consumer_group) in client.consumer_groups.iter().enumerate() {
            if consumer_group.stream_id == stream_id
                && consumer_group.topic_id == topic_id
                && consumer_group.group_id == consumer_group_id
            {
                client.consumer_groups.remove(index);
                return Ok(());
            }
        }
        Ok(())
    }

    pub async fn delete_consumer_groups_for_stream(&mut self, stream_id: u32) {
        for client in self.clients.values_mut() {
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

    pub fn delete_consumer_groups_for_topic(&mut self, stream_id: u32, topic_id: u32) {
        for client in self.clients.values_mut() {
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
