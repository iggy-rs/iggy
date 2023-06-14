use crate::binary::sender::Sender;
use crate::utils::binary_mapper;
use anyhow::Result;
use shared::error::Error;
use shared::topics::get_topic::GetTopic;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: GetTopic,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let topic = system
        .get_stream(command.stream_id)?
        .get_topic(command.topic_id)?;
    let topic = binary_mapper::map_topic(topic).await;
    sender.send_ok_response(&topic).await?;
    Ok(())
}
