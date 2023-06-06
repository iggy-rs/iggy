use crate::shared::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::topics::delete_topic::DeleteTopic;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: DeleteTopic,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let mut system = system.write().await;
    system
        .get_stream_mut(command.stream_id)?
        .delete_topic(command.topic_id)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
