use crate::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::topics::delete_topic::DeleteTopic;
use streaming::system::System;

pub async fn handle(
    command: DeleteTopic,
    sender: &mut Sender,
    system: &mut System,
) -> Result<(), Error> {
    system
        .get_stream_mut(command.stream_id)?
        .delete_topic(command.topic_id)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
