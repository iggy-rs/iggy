use crate::command::{Format, PollMessagesWithFormat};
use iggy::client::Client;
use iggy::client_error::ClientError;
use iggy::messages::send_messages::SendMessages;
use tracing::info;

pub async fn send_messages(
    command: &mut SendMessages,
    client: &dyn Client,
) -> Result<(), ClientError> {
    client.send_messages(command).await?;
    Ok(())
}

pub async fn poll_messages(
    command: &PollMessagesWithFormat,
    client: &dyn Client,
) -> Result<(), ClientError> {
    let format = command.format;
    let polled_messages = client.poll_messages(&command.payload).await?;
    if polled_messages.messages.is_empty() {
        info!("No messages found");
        return Ok(());
    }

    let mut text = format!("Received {} messages.", polled_messages.messages.len());
    if format == Format::None {
        info!("{}", text);
        return Ok(());
    }

    for message in polled_messages.messages {
        text += &format!(
            "\noffset: {}, timestamp: {}, ID: {}, length: {}, payload: ",
            message.offset, message.timestamp, message.id, message.length
        );
        match format {
            Format::Binary => text += &format!("{:?}", message.payload),
            Format::String => text += String::from_utf8_lossy(&message.payload).as_ref(),
            _ => {}
        }
    }

    info!("{}", text);
    Ok(())
}
