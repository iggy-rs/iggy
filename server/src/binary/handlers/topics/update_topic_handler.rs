use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::update_topic::UpdateTopic;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &UpdateTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write().await;
    system
        .update_topic(
            session,
            &command.stream_id,
            &command.topic_id,
            &command.name,
            command.message_expiry,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
