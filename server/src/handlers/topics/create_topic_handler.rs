use crate::handlers::STATUS_OK;
use anyhow::Result;
use shared::error::Error;
use shared::topics::create_topic::CreateTopic;
use std::net::SocketAddr;
use streaming::system::System;
use tokio::net::UdpSocket;

pub async fn handle(
    command: CreateTopic,
    socket: &UdpSocket,
    address: SocketAddr,
    system: &mut System,
) -> Result<(), Error> {
    system
        .get_stream_mut(command.stream_id)?
        .create_topic(command.topic_id, &command.name, command.partitions_count)
        .await?;
    socket.send_to(STATUS_OK, address).await?;
    Ok(())
}
