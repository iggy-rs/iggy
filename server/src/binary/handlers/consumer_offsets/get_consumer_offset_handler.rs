use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &GetConsumerOffset,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    if !user_context.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let system = system.read().await;
    let stream = system.get_stream(&command.stream_id)?;
    let topic = stream.get_topic(&command.topic_id)?;
    system.permissioner.get_consumer_offset(
        user_context.user_id,
        stream.stream_id,
        topic.topic_id,
    )?;

    let consumer = PollingConsumer::from_consumer(
        &command.consumer,
        user_context.client_id,
        command.partition_id,
    );
    let offset = topic.get_consumer_offset(consumer).await?;
    let offset = mapper::map_consumer_offset(&offset);
    sender.send_ok_response(&offset).await?;
    Ok(())
}
