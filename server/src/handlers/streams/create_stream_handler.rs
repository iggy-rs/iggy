use crate::handlers::STATUS_OK;
use anyhow::Result;
use shared::error::Error;
use shared::streams::create_stream::CreateStream;
use std::net::SocketAddr;
use streaming::system::System;
use tokio::net::UdpSocket;

pub async fn handle(
    command: CreateStream,
    socket: &UdpSocket,
    address: SocketAddr,
    system: &mut System,
) -> Result<(), Error> {
    system
        .create_stream(command.stream_id, &command.name)
        .await?;
    socket.send_to(STATUS_OK, address).await?;
    Ok(())
}
