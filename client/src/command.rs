use crate::client_error::ClientError;
use crate::handlers::messages::*;
use crate::handlers::offsets::store_offset_handler;
use crate::handlers::streams::*;
use crate::handlers::system::*;
use crate::handlers::topics::*;
use sdk::client::Client;
use shared::command::Command;
use std::str::FromStr;
use tracing::info;

pub async fn handle(input: &str, client: &dyn Client) -> Result<(), ClientError> {
    let command = Command::from_str(input).map_err(|_| ClientError::InvalidCommand)?;
    info!("Handling '{}' command...", command);
    match command {
        Command::Kill(payload) => kill_handler::handle(payload, client).await,
        Command::Ping(payload) => ping_handler::handle(payload, client).await,
        Command::SendMessages(payload) => send_messages_handler::handle(payload, client).await,
        Command::PollMessages(payload) => poll_messages_handler::handle(payload, client).await,
        Command::StoreOffset(payload) => store_offset_handler::handle(payload, client).await,
        Command::GetStreams(payload) => get_streams_handler::handle(payload, client).await,
        Command::CreateStream(payload) => create_stream_handler::handle(payload, client).await,
        Command::DeleteStream(payload) => delete_stream_handler::handle(payload, client).await,
        Command::GetTopics(payload) => get_topics_handler::handle(payload, client).await,
        Command::CreateTopic(payload) => create_topic_handler::handle(payload, client).await,
        Command::DeleteTopic(payload) => delete_topic_handler::handle(payload, client).await,
    }
}
