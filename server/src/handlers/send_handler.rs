use crate::handlers::STATUS_OK;
use anyhow::Result;
use std::net::SocketAddr;
use streaming::message::Message;
use streaming::stream_error::StreamError;
use streaming::system::System;
use tokio::net::UdpSocket;
use tracing::info;

pub const COMMAND: &[u8] = &[3];
const LENGTH: usize = 10;

pub async fn handle(
    input: &[u8],
    socket: &UdpSocket,
    address: SocketAddr,
    system: &mut System,
) -> Result<(), StreamError> {
    if input.len() < LENGTH {
        return Err(StreamError::InvalidCommand);
    }

    let topic = u32::from_le_bytes(input[..4].try_into().unwrap());
    let key_kind = input[4];
    let key_value = u32::from_le_bytes(input[5..9].try_into().unwrap());
    let payload = &input[9..];
    info!(
        "Sending message to topic: {:?}, key kind: {:?}, key value: {:?}, payload: {:?}",
        topic, key_kind, key_value, payload
    );

    let message = Message::new(payload.to_vec());
    system
        .stream
        .send_message(topic, key_value, message)
        .await?;
    socket.send_to(STATUS_OK, address).await?;
    Ok(())
}
