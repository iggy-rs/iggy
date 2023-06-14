use crate::binary::sender::Sender;
use crate::utils::binary_mapper;
use anyhow::Result;
use shared::error::Error;
use shared::streams::get_stream::GetStream;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: GetStream,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let stream = system.get_stream(command.stream_id)?;
    let stream = binary_mapper::map_stream(stream);
    sender.send_ok_response(&stream).await?;
    Ok(())
}
