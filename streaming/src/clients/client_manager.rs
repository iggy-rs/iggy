use crate::utils::checksum;
use iggy::error::Error;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct ClientManager {
    clients: HashMap<u32, Arc<RwLock<Client>>>,
}

#[derive(Debug)]
pub struct Client {
    pub id: u32,
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

impl Default for ClientManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientManager {
    pub fn new() -> ClientManager {
        ClientManager {
            clients: HashMap::new(),
        }
    }

    pub fn add_client(&mut self, address: &SocketAddr, transport: Transport) -> u32 {
        let id = checksum::calculate(address.to_string().as_bytes());
        let client = Client {
            id,
            address: *address,
            transport,
            consumer_groups: Vec::new(),
        };
        self.clients
            .insert(client.id, Arc::new(RwLock::new(client)));
        id
    }

    pub fn get_client_by_address(
        &self,
        address: &SocketAddr,
    ) -> Result<Arc<RwLock<Client>>, Error> {
        let id = checksum::calculate(address.to_string().as_bytes());
        self.get_client_by_id(id)
    }

    pub fn get_client_by_id(&self, client_id: u32) -> Result<Arc<RwLock<Client>>, Error> {
        let client = self.clients.get(&client_id);
        if client.is_none() {
            return Err(Error::ClientNotFound(client_id));
        }

        Ok(client.unwrap().clone())
    }

    pub fn get_clients(&self) -> Vec<Arc<RwLock<Client>>> {
        self.clients.values().cloned().collect()
    }

    pub fn delete_client(&mut self, address: &SocketAddr) -> Option<Arc<RwLock<Client>>> {
        let id = checksum::calculate(address.to_string().as_bytes());
        self.clients.remove(&id)
    }

    pub async fn join_consumer_group(
        &self,
        client_id: u32,
        stream_id: u32,
        topic_id: u32,
        consumer_group_id: u32,
    ) -> Result<(), Error> {
        let client = self.clients.get(&client_id);
        if client.is_none() {
            return Err(Error::ClientNotFound(client_id));
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
    ) -> Result<(), Error> {
        let client = self.clients.get(&client_id);
        if client.is_none() {
            return Err(Error::ClientNotFound(client_id));
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
}
