use crate::handlers::STATUS_OK;
use anyhow::Result;
use shared::error::Error;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

pub async fn handle(socket: &UdpSocket, address: SocketAddr) -> Result<(), Error> {
    socket.send_to(STATUS_OK, address).await?;
    Ok(())
}
