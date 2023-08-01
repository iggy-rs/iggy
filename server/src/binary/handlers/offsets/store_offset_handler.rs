use crate::binary::client_context::ClientContext;
use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::consumer_type::ConsumerType;
use iggy::error::Error;
use iggy::offsets::store_offset::StoreOffset;
use std::sync::Arc;
use streaming::polling_consumer::PollingConsumer;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &StoreOffset,
    sender: &mut dyn Sender,
    client_context: &ClientContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let consumer = match command.consumer_type {
        ConsumerType::Consumer => PollingConsumer::Consumer(command.consumer_id),
        ConsumerType::ConsumerGroup => {
            PollingConsumer::ConsumerGroup(command.consumer_id, client_context.client_id)
        }
    };

    let system = system.read().await;
    let topic = system
        .get_stream_by_id(command.stream_id)?
        .get_topic_by_id(command.topic_id)?;

    let partition_id = match consumer {
        PollingConsumer::Consumer(_) => command.partition_id,
        PollingConsumer::ConsumerGroup(consumer_group_id, member_id) => {
            let consumer_group = topic.get_consumer_group(consumer_group_id)?.read().await;
            consumer_group.get_current_partition_id(member_id).await?
        }
    };

    topic
        .store_offset(consumer, partition_id, command.offset)
        .await?;

    sender.send_empty_ok_response().await?;
    Ok(())
}
