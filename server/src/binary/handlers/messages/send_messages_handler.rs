use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::error::Error;
use iggy::messages::send_messages::SendMessages;
use std::sync::Arc;
use streaming::message::Message;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &SendMessages,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let mut messages = Vec::with_capacity(command.messages.len());
    for message in &command.messages {
        messages.push(Message::from_message(message));
    }
    let system = system.read().await;
    let stream = system.get_stream(&command.stream_id)?;
    let topic = stream.get_topic(&command.topic_id)?;
    topic.append_messages(&command.key, messages).await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
