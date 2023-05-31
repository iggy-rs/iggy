use crate::client_error::ClientError;
use sdk::client::Client;
use shared::messages::poll_messages::{Format, PollMessages};
use std::str::from_utf8;
use tracing::info;

pub async fn handle(command: PollMessages, client: &dyn Client) -> Result<(), ClientError> {
    let messages = client.poll_messages(&command).await?;
    if messages.is_empty() {
        info!("No messages found");
        return Ok(());
    }

    let mut text = format!("Received {} messages.", messages.len());
    if command.format == Format::None {
        info!("{}", text);
        return Ok(());
    }

    for message in messages {
        text += &format!(
            "\noffset: {}, timestamp: {}, length: {}, payload: ",
            message.offset, message.timestamp, message.length
        );
        match command.format {
            Format::Binary => text += &format!("{:?}", message.payload),
            Format::String => text += from_utf8(&message.payload).unwrap(),
            _ => {}
        }
    }

    info!("{}", text);
    Ok(())
}
