use anyhow::Result;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use streaming::stream::Stream;
use streaming::stream_error::StreamError;
use crate::handlers::STATUS_OK;

pub const COMMAND: &[u8] = &[12];
const LENGTH: usize = 4;

pub async fn handle(input: &[u8], socket: &UdpSocket, address: SocketAddr, stream: &mut Stream) -> Result<(), StreamError> {
    if input.len() != LENGTH {
        return Err(StreamError::InvalidCommand);
    }

    let id = u32::from_le_bytes(input[..4].try_into().unwrap());
    stream.delete_topic(id).await?;
    socket.send_to(STATUS_OK, address).await?;
    Ok(())
}