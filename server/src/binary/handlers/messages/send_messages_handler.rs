use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::error::Error;
use iggy::messages::send_messages::SendMessages;
use std::sync::Arc;
use streaming::systems::system::System;
use streaming::users::user_context::UserContext;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &SendMessages,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    system
        .append_messages(
            user_context,
            &command.stream_id,
            &command.topic_id,
            &command.partitioning,
            &command.messages,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
