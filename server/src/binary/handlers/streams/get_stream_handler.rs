use crate::binary::mapper;
use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::error::Error;
use iggy::streams::get_stream::GetStream;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &GetStream,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let stream = system.get_stream(command.stream_id)?;
    let stream = mapper::map_stream(stream).await;
    sender.send_ok_response(&stream).await?;
    Ok(())
}
