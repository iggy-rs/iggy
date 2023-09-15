use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::messages::send_messages::SendMessages;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &SendMessages,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    let system = system.read().await;
    let stream = system.get_stream(&command.stream_id)?;
    let topic = stream.get_topic(&command.topic_id)?;
    system
        .permissioner
        .append_messages(user_context.user_id, stream.stream_id, topic.topic_id)?;

    system
        .append_messages(
            &command.stream_id,
            &command.topic_id,
            &command.partitioning,
            &command.messages,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
