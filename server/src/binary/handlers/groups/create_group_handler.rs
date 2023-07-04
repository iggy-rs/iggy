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
        .create_consumer_group(command.stream_id, command.topic_id, command.group_id)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
