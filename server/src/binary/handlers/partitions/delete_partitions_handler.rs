use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::error::Error;
use iggy::partitions::delete_partitions::DeletePartitions;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &DeletePartitions,
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
            .delete_partitions(session.user_id, stream.stream_id, topic.topic_id)?;
    }

    let mut system = system.write().await;
    system
        .delete_partitions(
            &command.stream_id,
            &command.topic_id,
            command.partitions_count,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
