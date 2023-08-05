use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::error::Error;
use std::sync::Arc;
use streaming::systems::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &CreateConsumerGroup,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let mut system = system.write().await;
    system
        .create_consumer_group(
            &command.stream_id,
            &command.topic_id,
            command.consumer_group_id,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
