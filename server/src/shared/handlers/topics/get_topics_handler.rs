use crate::shared::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::topics::get_topics::GetTopics;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: GetTopics,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let topics = system
        .get_stream(command.stream_id)?
        .get_topics()
        .iter()
        .flat_map(|topic| {
            [
                &topic.id.to_le_bytes(),
                &(topic.get_partitions().len() as u32).to_le_bytes(),
                &(topic.name.len() as u32).to_le_bytes(),
                topic.name.as_bytes(),
            ]
            .concat()
        })
        .collect::<Vec<u8>>();

    sender.send_ok_response(&topics).await?;
    Ok(())
}
