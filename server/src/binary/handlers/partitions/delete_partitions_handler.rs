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
    let mut system = system.write().await;
    system
        .delete_partitions(
            session,
            &command.stream_id,
            &command.topic_id,
            command.partitions_count,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
