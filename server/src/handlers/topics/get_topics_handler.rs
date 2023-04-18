use crate::handlers::STATUS_OK;
use anyhow::Result;
use shared::error::Error;
use shared::topics::get_topics::GetTopics;
use std::net::SocketAddr;
use streaming::system::System;
use tokio::net::UdpSocket;

pub async fn handle(
    command: GetTopics,
    socket: &UdpSocket,
    address: SocketAddr,
    system: &mut System,
) -> Result<(), Error> {
    let topics = system
        .get_stream(command.stream_id)?
        .get_topics()
        .iter()
        .flat_map(|topic| {
            [
                &topic.id.to_le_bytes(),
                &(topic.get_partitions().len() as u32).to_le_bytes(),
                &(topic.name.len() as u32).to_le_bytes(),
                topic.name.as_bytes(),
            ]
            .concat()
        })
        .collect::<Vec<u8>>();

    socket
        .send_to([STATUS_OK, topics.as_slice()].concat().as_slice(), address)
        .await?;
    Ok(())
}
