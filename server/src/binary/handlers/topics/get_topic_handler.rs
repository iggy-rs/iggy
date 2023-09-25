use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::get_topic::GetTopic;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &GetTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let topic = system.find_topic(session, &command.stream_id, &command.topic_id)?;
    let topic = mapper::map_topic(topic).await;
    sender.send_ok_response(&topic).await?;
    Ok(())
}
