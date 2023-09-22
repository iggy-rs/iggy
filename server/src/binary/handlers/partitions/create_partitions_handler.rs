use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::error::Error;
use iggy::partitions::create_partitions::CreatePartitions;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &CreatePartitions,
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
        let topic = stream.get_topic(&command.topic_id)?;
        system
            .permissioner
            .create_partitons(session.user_id, stream.stream_id, topic.topic_id)?;
    }

    let mut system = system.write().await;
    let topic = system
        .get_stream_mut(&command.stream_id)?
        .get_topic_mut(&command.topic_id)?;
    topic
        .add_persisted_partitions(command.partitions_count)
        .await?;
    topic.reassign_consumer_groups().await;
    sender.send_empty_ok_response().await?;
    Ok(())
}
