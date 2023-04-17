use crate::client_error::ClientError;
use crate::handlers::messages::*;
use crate::handlers::streams::*;
use crate::handlers::system::*;
use crate::handlers::topics::*;
use sdk::client::Client;
use tracing::info;

pub async fn handle(input: &str, client: &mut Client) -> Result<(), ClientError> {
    let parts = input.split('|').collect::<Vec<&str>>();
    let command = parts[0];
    info!("Handling '{:#}' command...", command);
    if parts.len() == 1 {
        match command {
            "ping" => ping_handler::handle(client).await,
            "stream.list" => get_streams_handler::handle(client).await,
            _ => Err(ClientError::InvalidCommand),
        }
    } else {
        let input = &parts[1..];
        match command {
            "stream.create" => create_stream_handler::handle(client, input).await,
            "stream.delete" => delete_stream_handler::handle(client, input).await,
            "topic.list" => get_topics_handler::handle(client, input).await,
            "topic.create" => create_topic_handler::handle(client, input).await,
            "topic.delete" => delete_topic_handler::handle(client, input).await,
            "message.poll" => poll_handler::handle(client, input).await,
            "message.send" => send_handler::handle(client, input).await,
            _ => Err(ClientError::InvalidCommand),
        }
    }
}
