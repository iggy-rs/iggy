use crate::client_error::ClientError;
use crate::handlers::messages::*;
use crate::handlers::streams::*;
use crate::handlers::system::*;
use crate::handlers::topics::*;
use sdk::client::ConnectedClient;
use shared::command::Command;
use shared::messages::poll_messages::PollMessages;
use shared::messages::send_message::SendMessage;
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;
use shared::topics::create_topic::CreateTopic;
use shared::topics::delete_topic::DeleteTopic;
use shared::topics::get_topics::GetTopics;
use std::str::FromStr;
use tracing::info;

pub async fn handle(input: &str, client: &mut ConnectedClient) -> Result<(), ClientError> {
    let (command, input) = input.split_once('|').unwrap_or((input, ""));
    let command = Command::from_str(command).map_err(|_| ClientError::InvalidCommand)?;
    info!("Handling '{:?}' command...", command);
    match command {
        Command::Ping => ping_handler::handle(client).await,
        Command::GetStreams => get_streams_handler::handle(client).await,
        Command::SendMessage => {
            let command = SendMessage::from_str(input)?;
            send_message_handler::handle(command, client).await
        }
        Command::PollMessages => {
            let command = PollMessages::from_str(input)?;
            poll_messages_handler::handle(command, client).await
        }
        Command::CreateStream => {
            let command = CreateStream::from_str(input)?;
            create_stream_handler::handle(command, client).await
        }
        Command::DeleteStream => {
            let command = DeleteStream::from_str(input)?;
            delete_stream_handler::handle(command, client).await
        }
        Command::GetTopics => {
            let command = GetTopics::from_str(input)?;
            get_topics_handler::handle(command, client).await
        }
        Command::CreateTopic => {
            let command = CreateTopic::from_str(input)?;
            create_topic_handler::handle(command, client).await
        }
        Command::DeleteTopic => {
            let command = DeleteTopic::from_str(input)?;
            delete_topic_handler::handle(command, client).await
        }
    }
}
