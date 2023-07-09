use crate::binary::client_context::ClientContext;
use crate::binary::sender::Sender;
use anyhow::Result;
use sdk::consumer_type::ConsumerType;
use sdk::error::Error;
use sdk::offsets::store_offset::StoreOffset;
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
        ConsumerType::Group => {
            PollingConsumer::Group(command.consumer_id, client_context.client_id)
        }
    };

    let system = system.read().await;
    let topic = system
        .get_stream(command.stream_id)?
        .get_topic(command.topic_id)?;

    let partition_id = match consumer {
        PollingConsumer::Consumer(_) => command.partition_id,
        PollingConsumer::Group(group_id, member_id) => {
            let group = topic.get_consumer_group(group_id)?.read().await;
            group.get_current_partition_id(member_id).await?
        }
    };

    topic
        .store_offset(consumer, partition_id, command.offset)
        .await?;

    sender.send_empty_ok_response().await?;
    Ok(())
}
