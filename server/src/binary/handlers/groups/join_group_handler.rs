use crate::binary::client_context::ClientContext;
use crate::binary::sender::Sender;
use anyhow::Result;
use sdk::error::Error;
use sdk::groups::join_group::JoinGroup;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: JoinGroup,
    sender: &mut dyn Sender,
    client_context: &ClientContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    system
        .get_stream(command.stream_id)?
        .get_topic(command.topic_id)?
        .join_consumer_group(command.group_id, client_context.client_id)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
