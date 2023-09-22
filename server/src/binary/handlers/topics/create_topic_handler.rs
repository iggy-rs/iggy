use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::create_topic::CreateTopic;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &CreateTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    if !session.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    {
        let system = system.read().await;
        let stream = system.get_stream(&command.stream_id)?;
        system
            .permissioner
            .create_topic(session.user_id, stream.stream_id)?;
    }

    let mut system = system.write().await;
    system
        .get_stream_mut(&command.stream_id)?
        .create_topic(
            command.topic_id,
            &command.name,
            command.partitions_count,
            command.message_expiry,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
