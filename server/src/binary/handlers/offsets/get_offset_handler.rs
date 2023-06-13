use crate::binary::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::offsets::get_offset::GetOffset;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: GetOffset,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let offset = system
        .get_stream(command.stream_id)?
        .get_offset(command.consumer_id, command.topic_id, command.partition_id)
        .await?;

    let mut bytes = Vec::with_capacity(12);
    bytes.extend(command.consumer_id.to_le_bytes());
    bytes.extend(offset.to_le_bytes());
    sender.send_ok_response(&bytes).await?;
    Ok(())
}
