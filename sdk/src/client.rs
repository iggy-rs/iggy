use crate::error::Error;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tracing::{error, info};

const NAME: &str = "Iggy";

pub struct Client {
    pub(crate) socket: UdpSocket,
    pub(crate) buffer: [u8; 1024],
    pub(crate) address: SocketAddr,
    pub(crate) server: SocketAddr,
}

impl Client {
    pub async fn new(address: &str, server: &str) -> Result<Self, Error> {
        let address = address.parse::<SocketAddr>().unwrap();
        let server = server.parse::<SocketAddr>().unwrap();
        let socket = UdpSocket::bind(address).await?;
        let buffer = [0; 1024];
        Ok(Self {
            socket,
            buffer,
            address,
            server,
        })
    }

    pub async fn connect(&self) -> Result<(), Error> {
        self.socket.connect(self.server).await?;
        info!(
            "{} client has started on: {:?}, server address: {:?}",
            NAME, self.address, self.server
        );

        Ok(())
    }

    pub(crate) async fn send(&mut self, buffer: &[u8]) -> Result<(), Error> {
        self.socket.send(buffer).await?;
        self.handle_response().await?;
        Ok(())
    }

    pub(crate) async fn send_with_response(&mut self, buffer: &[u8]) -> Result<&[u8], Error> {
        self.socket.send(buffer).await?;
        self.handle_response_with_payload().await
    }

    async fn handle_response(&mut self) -> Result<(), Error> {
        self.handle_response_with_payload().await?;
        Ok(())
    }

    async fn handle_response_with_payload(&mut self) -> Result<&[u8], Error> {
        let length = self.socket.recv(&mut self.buffer).await?;
        if self.buffer.is_empty() {
            return Err(Error::EmptyResponse);
        }

        let status = self.buffer[0];
        if status == 0 {
            info!("Status: OK.");
            return Ok(&self.buffer[1..length]);
        }

        error!("Received an invalid response with status: {:?}.", status);
        Err(Error::InvalidResponse(status))
    }
}
