use std::net::SocketAddr;

use async_channel::Sender;
use bytes::Bytes;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;

use crate::command::ServerCommand;

#[derive(Debug, Clone)]
pub enum ShardMessage {
    Command(ServerCommand),
    Event(ShardEvent),
}

#[derive(Debug, Clone)]
pub enum ShardEvent {
    CreatedStream(Option<u32>, String),
    CreatedPartitions(Identifier, Identifier, u32),
    CreatedTopic(
        Identifier,
        Option<u32>,
        String,
        u32,
        IggyExpiry,
        CompressionAlgorithm,
        MaxTopicSize,
        Option<u8>,
    ),
    LoginUser(String, String),
    NewSession(u32, SocketAddr),
}

#[derive(Debug)]
pub enum ShardResponse {
    BinaryResponse(Bytes),
    ErrorResponse(IggyError),
}

#[derive(Debug, Clone)]
pub struct ShardFrame {
    pub client_id: u32,
    pub message: ShardMessage,
    pub response_sender: Option<Sender<ShardResponse>>,
}

impl ShardFrame {
    pub fn new(
        client_id: u32,
        message: ShardMessage,
        response_sender: Option<Sender<ShardResponse>>,
    ) -> Self {
        Self {
            client_id,
            message,
            response_sender,
        }
    }
}

#[macro_export]
macro_rules! handle_response {
    ($sender:expr, $response:expr) => {
        match $response {
            ShardResponse::BinaryResponse(payload) => $sender.send_ok_response(&payload).await?,
            ShardResponse::ErrorResponse(err) => $sender.send_error_response(err).await?,
        }
    };
}
