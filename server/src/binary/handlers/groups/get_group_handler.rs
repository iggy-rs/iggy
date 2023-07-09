use crate::binary::mapper;
use crate::binary::sender::Sender;
use anyhow::Result;
use sdk::error::Error;
use sdk::groups::get_group::GetGroup;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &GetGroup,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let consumer_group = system
        .get_stream(command.stream_id)?
        .get_topic(command.topic_id)?
        .get_consumer_group(command.group_id)?;
    let consumer_group = consumer_group.read().await;
    let consumer_group = mapper::map_consumer_group(&consumer_group).await;
    sender.send_ok_response(consumer_group.as_slice()).await?;
    Ok(())
}
