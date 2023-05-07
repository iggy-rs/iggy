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
        "Storing offset: {} for stream: {}, topic: {}, partition: {}...",
        command.offset,
        command.stream_id,
        command.topic_id,
        command.partition_id,
    );

    // TODO: Store offset

    trace!(
        "Stored offset: {} for stream: {}, topic: {}, partition: {}.",
        command.offset,
        command.stream_id,
        command.topic_id,
        command.partition_id,
    );

    sender.send_empty_ok_response().await?;
    Ok(())
}
