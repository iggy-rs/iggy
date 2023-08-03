use crate::binary::mapper;
use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::get_topics::GetTopics;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &GetTopics,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let topics = system.get_stream(&command.stream_id)?.get_topics();
    let topics = mapper::map_topics(&topics).await;
    sender.send_ok_response(&topics).await?;
    Ok(())
}
