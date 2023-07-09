use crate::binary::mapper;
use crate::binary::sender::Sender;
use anyhow::Result;
use sdk::error::Error;
use sdk::streams::get_streams::GetStreams;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &GetStreams,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let streams = system.get_streams();
    let streams = mapper::map_streams(&streams);
    sender.send_ok_response(streams.as_slice()).await?;
    Ok(())
}
