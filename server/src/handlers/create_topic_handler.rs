use crate::handlers::STATUS_OK;
use anyhow::Result;
use std::net::SocketAddr;
use std::str::from_utf8;
use streaming::error::Error;
use streaming::system::System;
use tokio::net::UdpSocket;

pub const COMMAND: &[u8] = &[21];
const LENGTH: usize = 13;

pub async fn handle(
    input: &[u8],
    socket: &UdpSocket,
    address: SocketAddr,
    system: &mut System,
) -> Result<(), Error> {
    if input.len() < LENGTH {
        return Err(Error::InvalidCommand);
    }

    let stream = u32::from_le_bytes(input[..4].try_into().unwrap());
    let topic = u32::from_le_bytes(input[4..8].try_into().unwrap());
    let partitions = u32::from_le_bytes(input[8..12].try_into().unwrap());
    let name = from_utf8(&input[12..]).unwrap();
    if name.len() > 100 {
        return Err(Error::InvalidTopicName);
    }

    if !(1..=100).contains(&partitions) {
        return Err(Error::InvalidTopicPartitions);
    }

    system
        .get_stream_mut(stream)?
        .create_topic(topic, name, partitions)
        .await?;
    socket.send_to(STATUS_OK, address).await?;
    Ok(())
}
