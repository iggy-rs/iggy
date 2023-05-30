use crate::quic::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::offsets::store_offset::StoreOffset;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: StoreOffset,
    sender: &mut Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    system
        .get_stream(command.stream_id)?
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
