use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::consumer_groups::get_consumer_groups::GetConsumerGroups;
use iggy::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &GetConsumerGroups,
    sender: &mut dyn Sender,
    session: &Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let consumer_groups =
        system.get_consumer_groups(session, &command.stream_id, &command.topic_id)?;
    let consumer_groups = mapper::map_consumer_groups(&consumer_groups).await;
    sender.send_ok_response(consumer_groups.as_slice()).await?;
    Ok(())
}
