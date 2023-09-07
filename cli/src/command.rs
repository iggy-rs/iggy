use crate::{
    consumer_groups, consumer_offsets, messages, partitions, streams, system, topics, users,
};
use iggy::client_error::ClientError;
use iggy::clients::client::IggyClient;
use iggy::command::Command;
use iggy::messages::poll_messages::PollMessages;
use std::str::FromStr;
use tracing::info;

pub async fn handle(input: &str, client: &IggyClient) -> Result<(), ClientError> {
    let command = Command::from_str(input).map_err(|_| ClientError::InvalidCommand)?;
    info!("Handling '{}' command...", command);
    match command {
        Command::Ping(payload) => system::ping(&payload, client).await,
        Command::GetStats(payload) => system::get_stats(&payload, client).await,
        Command::GetMe(payload) => system::get_me(&payload, client).await,
        Command::GetClient(payload) => system::get_client(&payload, client).await,
        Command::GetClients(payload) => system::get_clients(&payload, client).await,
        Command::LoginUser(payload) => users::login_user(&payload, client).await,
        Command::SendMessages(mut payload) => messages::send_messages(&mut payload, client).await,
        Command::PollMessages(payload) => {
            let format = match input.split('|').last() {
                Some(format) => match format {
                    "b" | "binary" => Format::Binary,
                    "s" | "string" => Format::String,
                    _ => Format::None,
                },
                None => Format::None,
            };
            let payload = PollMessagesWithFormat { payload, format };
            messages::poll_messages(&payload, client).await
        }
        Command::StoreConsumerOffset(payload) => {
            consumer_offsets::store_consumer_offset(&payload, client).await
        }
        Command::GetConsumerOffset(payload) => {
            consumer_offsets::get_consumer_offset(&payload, client).await
        }
        Command::GetStream(payload) => streams::get_stream(&payload, client).await,
        Command::GetStreams(payload) => streams::get_streams(&payload, client).await,
        Command::CreateStream(payload) => streams::create_stream(&payload, client).await,
        Command::DeleteStream(payload) => streams::delete_stream(&payload, client).await,
        Command::UpdateStream(payload) => streams::update_stream(&payload, client).await,
        Command::GetTopic(payload) => topics::get_topic(&payload, client).await,
        Command::GetTopics(payload) => topics::get_topics(&payload, client).await,
        Command::CreateTopic(payload) => topics::create_topic(&payload, client).await,
        Command::DeleteTopic(payload) => topics::delete_topic(&payload, client).await,
        Command::UpdateTopic(payload) => topics::update_topic(&payload, client).await,
        Command::CreatePartitions(payload) => partitions::create_partitions(&payload, client).await,
        Command::DeletePartitions(payload) => partitions::delete_partitions(&payload, client).await,
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

#[derive(Debug)]
pub struct PollMessagesWithFormat {
    pub payload: PollMessages,
    pub format: Format,
}

#[derive(Debug, PartialEq, Default, Copy, Clone)]
pub enum Format {
    #[default]
    None,
    Binary,
    String,
}
