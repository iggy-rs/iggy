use crate::utils::checksum;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use tracing::info;

#[derive(Debug)]
pub struct ClientManager {
    clients: HashMap<u32, Client>,
}

#[derive(Debug)]
pub struct Client {
    pub id: u32,
    pub address: SocketAddr,
    pub transport: Transport,
}

#[derive(Debug)]
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

    pub fn add_client(&mut self, address: &SocketAddr, transport: Transport) {
        let id = checksum::get(address.to_string().as_bytes());
        info!("Added {transport} client with ID: {id} for address: {address}");
        let client = Client {
            id,
            address: *address,
            transport,
        };
        self.clients.insert(client.id, client);
    }

    pub fn remove_client(&mut self, address: &SocketAddr) {
        let id = checksum::get(address.to_string().as_bytes());
        let client = self.clients.remove(&id);
        if client.is_none() {
            return;
        }

        let client = client.unwrap();
        info!(
            "Removed {} client with ID: {} for address: {}",
            client.transport, id, client.address
        );
    }

    pub fn get_clients(&self) -> Vec<&Client> {
        self.clients.values().collect()
    }
}
