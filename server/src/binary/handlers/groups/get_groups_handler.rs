use crate::binary::mapper;
use crate::binary::sender::Sender;
use anyhow::Result;
use sdk::error::Error;
use sdk::groups::get_groups::GetGroups;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &GetGroups,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let topic = system
        .get_stream(command.stream_id)?
        .get_topic(command.topic_id)?;
    let consumer_groups = mapper::map_consumer_groups(&topic.get_consumer_groups()).await;
    sender.send_ok_response(consumer_groups.as_slice()).await?;
    Ok(())
}
