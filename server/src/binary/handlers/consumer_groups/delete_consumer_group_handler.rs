use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::error::Error;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &DeleteConsumerGroup,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let mut system = system.write().await;
    system
        .delete_consumer_group(
            &command.stream_id,
            &command.topic_id,
            command.consumer_group_id,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
