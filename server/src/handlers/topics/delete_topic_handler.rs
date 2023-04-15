use crate::handlers::STATUS_OK;
use anyhow::Result;
use std::net::SocketAddr;
use streaming::error::Error;
use streaming::system::System;
use tokio::net::UdpSocket;

pub const COMMAND: &[u8] = &[22];
const LENGTH: usize = 8;

pub async fn handle(
    input: &[u8],
    socket: &UdpSocket,
    address: SocketAddr,
    system: &mut System,
) -> Result<(), Error> {
    if input.len() != LENGTH {
        return Err(Error::InvalidCommand);
    }

    let stream = u32::from_le_bytes(input[..4].try_into().unwrap());
    let topic = u32::from_le_bytes(input[4..8].try_into().unwrap());
    system.get_stream_mut(stream)?.delete_topic(topic).await?;
    socket.send_to(STATUS_OK, address).await?;
    Ok(())
}
