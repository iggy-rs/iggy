use std::fmt::Display;
use std::net::SocketAddr;

use async_channel::Sender;
use bytes::Bytes;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::consumer::Consumer;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::models::permissions::Permissions;
use iggy::models::user_status::UserStatus;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;

use crate::command::ServerCommand;
use crate::streaming::clients::client_manager::Transport;
use crate::streaming::polling_consumer::PollingConsumer;

#[derive(Debug, Clone)]
pub enum ShardMessage {
    Command(ServerCommand),
    Event(ShardEvent),
}

#[derive(Debug, Clone)]
pub enum ShardEvent {
    CreatedStream(Option<u32>, String),
    DeletedStream(Identifier),
    UpdatedStream(Identifier, String),
    PurgedStream(Identifier),
    CreatedPartitions(Identifier, Identifier, u32),
    DeletedPartitions(Identifier, Identifier, u32),
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
    CreatedConsumerGroup(Identifier, Identifier, Option<u32>, String),
    DeletedConsumerGroup(Identifier, Identifier, Identifier),
    UpdatedTopic(
        Identifier,
        Identifier,
        String,
        IggyExpiry,
        CompressionAlgorithm,
        MaxTopicSize,
        Option<u8>,
    ),
    PurgedTopic(Identifier, Identifier),
    DeletedTopic(Identifier, Identifier),
    CreatedUser(String, String, UserStatus, Option<Permissions>),
    DeletedUser(Identifier),
    LoginUser(String, String),
    LogoutUser,
    UpdatedUser(Identifier, Option<String>, Option<UserStatus>),
    ChangedPassword(Identifier, String, String),
    CreatedPersonalAccessToken(String, IggyExpiry),
    DeletedPersonalAccessToken(String),
    LoginWithPersonalAccessToken(String),
    StoredConsumerOffset(Identifier, Identifier, PollingConsumer, u64),
    NewSession(u32, SocketAddr, Transport),
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
