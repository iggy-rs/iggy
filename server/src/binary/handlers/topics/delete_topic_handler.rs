use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::delete_topic::DeleteTopic;
use std::sync::Arc;
use streaming::systems::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &DeleteTopic,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let mut system = system.write().await;
    system
        .delete_topic(&command.stream_id, &command.topic_id)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
