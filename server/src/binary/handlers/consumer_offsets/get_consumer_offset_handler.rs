use crate::binary::client_context::ClientContext;
use crate::binary::mapper;
use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::error::Error;
use std::sync::Arc;
use streaming::polling_consumer::PollingConsumer;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &GetConsumerOffset,
    sender: &mut dyn Sender,
    client_context: &ClientContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let consumer = PollingConsumer::from_consumer(&command.consumer, client_context.client_id);
    let system = system.read().await;
    let offset = system
        .get_stream(&command.stream_id)?
        .get_topic(&command.topic_id)?
        .get_consumer_offset(consumer, command.partition_id)
        .await?;
    let offset = mapper::map_offset(command.consumer.id, offset);
    sender.send_ok_response(&offset).await?;
    Ok(())
}
