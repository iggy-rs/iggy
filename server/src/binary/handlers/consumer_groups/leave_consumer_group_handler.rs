use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use iggy::error::Error;
use std::sync::Arc;
use streaming::systems::system::System;
use streaming::users::user_context::UserContext;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &LeaveConsumerGroup,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    system
        .leave_consumer_group(
            user_context.client_id,
            &command.stream_id,
            &command.topic_id,
            command.consumer_group_id,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
