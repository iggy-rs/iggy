use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &DeleteConsumerGroup,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    {
        let system = system.read().await;
        let stream = system.get_stream(&command.stream_id)?;
        let topic = stream.get_topic(&command.topic_id)?;
        system.permissioner.delete_consumer_group(
            user_context.user_id,
            stream.stream_id,
            topic.topic_id,
        )?;
    }

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
