use crate::handlers::STATUS_OK;
use anyhow::Result;
use std::net::SocketAddr;
use streaming::error::Error;
use tokio::net::UdpSocket;

pub const COMMAND: &[u8] = &[1];

pub async fn handle(socket: &UdpSocket, address: SocketAddr) -> Result<(), Error> {
    socket.send_to(STATUS_OK, address).await?;
    Ok(())
}
