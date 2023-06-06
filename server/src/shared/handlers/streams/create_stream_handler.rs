use crate::shared::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::streams::create_stream::CreateStream;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: CreateStream,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let mut system = system.write().await;
    system
        .create_stream(command.stream_id, &command.name)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
