use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::update_topic::UpdateTopic;
use std::sync::Arc;
use streaming::systems::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &UpdateTopic,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let mut system = system.write().await;
    system
        .get_stream_mut(&command.stream_id)?
        .update_topic(&command.topic_id, &command.name, command.message_expiry)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
