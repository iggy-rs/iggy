use crate::quic::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::messages::send_messages::SendMessages;
use std::sync::Arc;
use streaming::message::Message;
use streaming::system::System;
use streaming::utils::timestamp;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: SendMessages,
    sender: &mut Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let mut messages = Vec::with_capacity(command.messages_count as usize);
    for message in command.messages {
        let timestamp = timestamp::get();
        messages.push(Message::empty(timestamp, message.id, message.payload));
    }

    system
        .read()
        .await
        .get_stream(command.stream_id)?
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
