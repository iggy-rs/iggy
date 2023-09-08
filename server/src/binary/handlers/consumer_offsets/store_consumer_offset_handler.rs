use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::error::Error;
use std::sync::Arc;
use streaming::polling_consumer::PollingConsumer;
use streaming::systems::system::System;
use streaming::users::user_context::UserContext;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &StoreConsumerOffset,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let consumer = PollingConsumer::from_consumer(
        &command.consumer,
        user_context.client_id,
        command.partition_id,
    );
    let system = system.read().await;
    system
        .get_stream(&command.stream_id)?
        .get_topic(&command.topic_id)?
        .store_consumer_offset(consumer, command.offset)
        .await?;

    sender.send_empty_ok_response().await?;
    Ok(())
}
