use crate::handlers::STATUS_OK;
use anyhow::Result;
use shared::error::Error;
use shared::topics::delete_topic::DeleteTopic;
use std::net::SocketAddr;
use streaming::system::System;
use tokio::net::UdpSocket;

pub async fn handle(
    command: DeleteTopic,
    socket: &UdpSocket,
    address: SocketAddr,
    system: &mut System,
) -> Result<(), Error> {
    system
        .get_stream_mut(command.stream_id)?
        .delete_topic(command.topic_id)
        .await?;
    socket.send_to(STATUS_OK, address).await?;
    Ok(())
}
