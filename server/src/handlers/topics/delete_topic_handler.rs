use crate::handlers::STATUS_OK;
use anyhow::Result;
use shared::error::Error;
use shared::topics::delete_topic::DeleteTopic;
use streaming::system::System;

pub async fn handle(
    command: DeleteTopic,
    send: &mut quinn::SendStream,
    system: &mut System,
) -> Result<(), Error> {
    system
        .get_stream_mut(command.stream_id)?
        .delete_topic(command.topic_id)
        .await?;
    send.write_all(STATUS_OK).await?;
    Ok(())
}
