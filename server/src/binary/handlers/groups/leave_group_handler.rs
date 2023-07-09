use crate::binary::client_context::ClientContext;
use crate::binary::sender::Sender;
use anyhow::Result;
use sdk::error::Error;
use sdk::groups::leave_group::LeaveGroup;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &LeaveGroup,
    sender: &mut dyn Sender,
    client_context: &ClientContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    system
        .leave_consumer_group(
            client_context.client_id,
            command.stream_id,
            command.topic_id,
            command.group_id,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
