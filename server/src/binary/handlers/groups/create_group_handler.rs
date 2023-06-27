use crate::binary::sender::Sender;
use anyhow::Result;
use sdk::error::Error;
use sdk::groups::create_group::CreateGroup;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: CreateGroup,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let mut system = system.write().await;
    system
        .get_stream_mut(command.stream_id)?
        .get_topic_mut(command.topic_id)?
        .create_consumer_group(command.group_id)?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
