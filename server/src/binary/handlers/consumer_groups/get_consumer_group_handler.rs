use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &GetConsumerGroup,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let stream = system.get_stream(&command.stream_id)?;
    let topic = stream.get_topic(&command.topic_id)?;
    system.permissioner.get_consumer_group(
        user_context.user_id,
        stream.stream_id,
        topic.topic_id,
    )?;
    let consumer_group = topic.get_consumer_group(command.consumer_group_id)?;
    let consumer_group = consumer_group.read().await;
    let consumer_group = mapper::map_consumer_group(&consumer_group).await;
    sender.send_ok_response(consumer_group.as_slice()).await?;
    Ok(())
}
