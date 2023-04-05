use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tracing::info;
use streaming::message::Message;
use streaming::stream::Stream;
use streaming::stream_error::StreamError;
use crate::handlers::STATUS_OK;

pub const COMMAND: &[u8] = &[3];
const LENGTH: usize = 10;

pub async fn handle(input: &[u8], socket: &UdpSocket, address: SocketAddr, stream: &mut Stream) -> Result<(), StreamError> {
    if input.len() < LENGTH {
        return Err(StreamError::InvalidCommand);
    }

    let topic = u32::from_le_bytes(input[..4].try_into().unwrap());
    let key_kind = input[4];
    let key_value = u32::from_le_bytes(input[5..9].try_into().unwrap());
    let payload = &input[9..];
    info!("Sending message to topic: {:?}, key kind: {:?}, key value: {:?}, payload: {:?}", topic, key_kind, key_value, payload);

    let message = Message::create(payload.to_vec());
    let stream_topic = stream.topics.get_mut(&topic);
    if stream_topic.is_none() {
        return Err(StreamError::TopicNotFound);
    }

    let stream_topic = stream_topic.unwrap();
    stream_topic.send_message(key_value, message).await?;
    if socket.send_to(STATUS_OK, address).await.is_err() {
        return Err(StreamError::NetworkError);
    }

    Ok(())
}