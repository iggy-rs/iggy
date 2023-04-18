use crate::client_error::ClientError;
use crate::handlers::messages::*;
use crate::handlers::streams::*;
use crate::handlers::system::*;
use crate::handlers::topics::*;
use sdk::client::Client;
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

pub async fn handle(input: &str, client: &mut Client) -> Result<(), ClientError> {
    let parts = input.split('|').collect::<Vec<&str>>();
    let command = Command::from_str(parts[0]);
    if command.is_err() {
        return Err(ClientError::InvalidCommand);
    }

    let command = command.unwrap();
    info!("Handling '{:?}' command...", command);
    if parts.len() == 1 {
        match command {
            Command::Ping => ping_handler::handle(client).await,
            Command::GetStreams => get_streams_handler::handle(client).await,
            _ => Err(ClientError::InvalidCommand),
        }
    } else {
        let input = &parts[1..];
        match command {
            Command::SendMessage => {
                let command = SendMessage::try_from(input)?;
                send_message_handler::handle(command, client).await
            }
            Command::PollMessages => {
                let command = PollMessages::try_from(input)?;
                poll_messages_handler::handle(command, client).await
            }
            Command::CreateStream => {
                let command = CreateStream::try_from(input)?;
                create_stream_handler::handle(command, client).await
            }
            Command::DeleteStream => {
                let command = DeleteStream::try_from(input)?;
                delete_stream_handler::handle(command, client).await
            }
            Command::GetTopics => {
                let command = GetTopics::try_from(input)?;
                get_topics_handler::handle(command, client).await
            }
            Command::CreateTopic => {
                let command = CreateTopic::try_from(input)?;
                create_topic_handler::handle(command, client).await
            }
            Command::DeleteTopic => {
                let command = DeleteTopic::try_from(input)?;
                delete_topic_handler::handle(command, client).await
            }
            _ => Err(ClientError::InvalidCommand),
        }
    }
}
