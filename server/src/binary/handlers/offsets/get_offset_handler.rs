use crate::binary::client_context::ClientContext;
use crate::binary::mapper;
use crate::binary::sender::Sender;
use anyhow::Result;
use sdk::consumer_type::ConsumerType;
use sdk::error::Error;
use sdk::offsets::get_offset::GetOffset;
use std::sync::Arc;
use streaming::polling_consumer::PollingConsumer;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: GetOffset,
    sender: &mut dyn Sender,
    client_context: &ClientContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let consumer = match command.consumer_type {
        ConsumerType::Consumer => PollingConsumer::Consumer(command.consumer_id),
        ConsumerType::Group => {
            PollingConsumer::Group(command.consumer_id, client_context.client_id)
        }
    };

    let system = system.read().await;
    let offset = system
        .get_stream(command.stream_id)?
        .get_topic(command.topic_id)?
        .get_offset(consumer, command.partition_id)
        .await?;
    let offset = mapper::map_offset(command.consumer_id, offset);
    sender.send_ok_response(&offset).await?;
    Ok(())
}
