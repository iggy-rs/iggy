use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &CreateConsumerGroup,
    sender: &mut dyn Sender,
    session: &Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write().await;
    system
        .create_consumer_group(
            session,
            &command.stream_id,
            &command.topic_id,
            command.consumer_group_id,
            &command.name,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
