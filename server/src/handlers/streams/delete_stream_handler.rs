use crate::handlers::STATUS_OK;
use anyhow::Result;
use shared::error::Error;
use shared::streams::delete_stream::DeleteStream;
use std::net::SocketAddr;
use streaming::system::System;
use tokio::net::UdpSocket;

pub async fn handle(
    command: DeleteStream,
    socket: &UdpSocket,
    address: SocketAddr,
    system: &mut System,
) -> Result<(), Error> {
    system.delete_stream(command.stream_id).await?;
    socket.send_to(STATUS_OK, address).await?;
    Ok(())
}
