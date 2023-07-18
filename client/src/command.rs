use crate::{consumer_groups, messages, offsets, streams, system, topics};
use iggy::client::Client;
use iggy::client_error::ClientError;
use iggy::command::Command;
use std::str::FromStr;
use tracing::info;

pub async fn handle(input: &str, client: &dyn Client) -> Result<(), ClientError> {
    let command = Command::from_str(input).map_err(|_| ClientError::InvalidCommand)?;
    info!("Handling '{}' command...", command);
    match command {
        Command::Kill(payload) => system::kill(&payload, client).await,
        Command::Ping(payload) => system::ping(&payload, client).await,
        Command::GetStats(payload) => system::get_stats(&payload, client).await,
        Command::GetMe(payload) => system::get_me(&payload, client).await,
        Command::GetClient(payload) => system::get_client(&payload, client).await,
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
        Command::GetGroup(payload) => consumer_groups::get_consumer_group(&payload, client).await,
        Command::GetGroups(payload) => consumer_groups::get_consumer_groups(&payload, client).await,
        Command::CreateGroup(payload) => {
            consumer_groups::create_consumer_group(&payload, client).await
        }
        Command::DeleteGroup(payload) => {
            consumer_groups::delete_consumer_group(&payload, client).await
        }
        Command::JoinGroup(payload) => consumer_groups::join_consumer_group(&payload, client).await,
        Command::LeaveGroup(payload) => {
            consumer_groups::leave_consumer_group(&payload, client).await
        }
    }
}
