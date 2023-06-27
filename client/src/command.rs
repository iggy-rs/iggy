use crate::{messages, offsets, streams, system, topics};
use sdk::client::Client;
use sdk::client_error::ClientError;
use sdk::command::Command;
use std::str::FromStr;
use tracing::info;

pub async fn handle(input: &str, client: &dyn Client) -> Result<(), ClientError> {
    let command = Command::from_str(input).map_err(|_| ClientError::InvalidCommand)?;
    info!("Handling '{}' command...", command);
    match command {
        Command::Kill(payload) => system::kill(&payload, client).await,
        Command::Ping(payload) => system::ping(&payload, client).await,
        Command::GetClients(payload) => system::get_clients(&payload, client).await,
        Command::SendMessages(payload) => messages::send_messages(&payload, client).await,
        Command::PollMessages(payload) => messages::poll_messages(&payload, client).await,
        Command::StoreOffset(payload) => offsets::store_offset(&payload, client).await,
        Command::GetOffset(payload) => offsets::get_offset(&payload, client).await,
        Command::GetStream(payload) => streams::get_stream(&payload, client).await,
        Command::GetStreams(payload) => streams::get_streams(&payload, client).await,
        Command::CreateStream(payload) => streams::create_stream(&payload, client).await,
        Command::DeleteStream(payload) => streams::delete_stream(&payload, client).await,
        Command::GetTopic(payload) => topics::get_topic(&payload, client).await,
        Command::GetTopics(payload) => topics::get_topics(&payload, client).await,
        Command::CreateTopic(payload) => topics::create_topic(&payload, client).await,
        Command::DeleteTopic(payload) => topics::delete_topic(&payload, client).await,
        Command::CreateGroup(payload) => topics::create_group(&payload, client).await,
        Command::DeleteGroup(payload) => topics::delete_group(&payload, client).await,
    }
}
