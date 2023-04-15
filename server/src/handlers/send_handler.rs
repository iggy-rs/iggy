use crate::handlers::STATUS_OK;
use anyhow::Result;
use std::net::SocketAddr;
use streaming::error::Error;
use streaming::message::Message;
use streaming::system::System;
use tokio::net::UdpSocket;
use tracing::info;

pub const COMMAND: &[u8] = &[3];
const LENGTH: usize = 14;

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
    let key_kind = input[8];
    let key_value = u32::from_le_bytes(input[9..13].try_into().unwrap());
    let payload = &input[13..];
    info!(
        "Sending message to stream: {:?}, topic: {:?}, key kind: {:?}, key value: {:?}, payload: {:?}",
        stream, topic, key_kind, key_value, payload
    );

    let message = Message::empty(payload.to_vec());
    system
        .get_stream_mut(stream)?
        .append_messages(topic, key_value, message)
        .await?;
    socket.send_to(STATUS_OK, address).await?;
    Ok(())
}
