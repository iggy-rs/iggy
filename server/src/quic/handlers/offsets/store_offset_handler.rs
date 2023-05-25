use crate::quic::sender::Sender;
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
    trace!("{}", command);
    system
        .get_stream_mut(command.stream_id)?
        .store_offset(
            command.consumer_id,
            command.topic_id,
            command.partition_id,
            command.offset,
        )
        .await?;

    sender.send_empty_ok_response().await?;
    Ok(())
}
