use crate::binary::sender::Sender;
use anyhow::Result;
use sdk::error::Error;
use sdk::messages::send_messages::SendMessages;
use std::sync::Arc;
use streaming::message::Message;
use streaming::system::System;
use streaming::utils::{checksum, timestamp};
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: SendMessages,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let mut messages = Vec::with_capacity(command.messages_count as usize);
    for message in command.messages {
        let timestamp = timestamp::get();
        let checksum = checksum::get(&message.payload);
        messages.push(Message::empty(
            timestamp,
            message.id,
            message.payload,
            checksum,
        ));
    }

    let system = system.read().await;
    system
        .get_stream(command.stream_id)?
        .append_messages(
            command.topic_id,
            command.key_kind,
            command.key_value,
            messages,
            system.storage.segment.clone(),
        )
        .await?;

    sender.send_empty_ok_response().await?;
    Ok(())
}
