use crate::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::offsets::store_offset::StoreOffset;
use streaming::system::System;
use tracing::trace;

pub async fn handle(
    command: StoreOffset,
    sender: &mut Sender,
    system: &mut System,
) -> Result<(), Error> {
    trace!(
        "Storing offset: {} for consumer: {}, stream: {}, topic: {}, partition: {}...",
        command.offset,
        command.consumer_id,
        command.stream_id,
        command.topic_id,
        command.partition_id,
    );

    system
        .get_stream_mut(command.stream_id)?
        .store_offset(
            command.consumer_id,
            command.topic_id,
            command.partition_id,
            command.offset,
        )
        .await?;

    trace!(
        "Stored offset: {} for consumer: {}, stream: {}, topic: {}, partition: {}.",
        command.offset,
        command.consumer_id,
        command.stream_id,
        command.topic_id,
        command.partition_id,
    );

    sender.send_empty_ok_response().await?;
    Ok(())
}
