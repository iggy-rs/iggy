use crate::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::topics::create_topic::CreateTopic;
use streaming::system::System;

pub async fn handle(
    command: CreateTopic,
    sender: &mut Sender,
    system: &mut System,
) -> Result<(), Error> {
    system
        .get_stream_mut(command.stream_id)?
        .create_topic(command.topic_id, &command.name, command.partitions_count)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
