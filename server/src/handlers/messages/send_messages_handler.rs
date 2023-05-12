use crate::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::messages::send_messages::SendMessages;
use streaming::message::Message;
use streaming::system::System;
use tracing::trace;

pub async fn handle(
    command: SendMessages,
    sender: &mut Sender,
    system: &mut System,
) -> Result<(), Error> {
    trace!("{}", command);
    let mut messages = Vec::with_capacity(command.messages_count as usize);
    for message in command.messages {
        messages.push(Message::create(0, 0, message.payload));
    }

    system
        .get_stream_mut(command.stream_id)?
        .append_messages(
            command.topic_id,
            command.key_kind,
            command.key_value,
            messages,
        )
        .await?;

    sender.send_empty_ok_response().await?;
    Ok(())
}
