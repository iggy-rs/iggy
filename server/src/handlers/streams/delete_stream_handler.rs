use crate::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::streams::delete_stream::DeleteStream;
use streaming::system::System;

pub async fn handle(
    command: DeleteStream,
    sender: &mut Sender,
    system: &mut System,
) -> Result<(), Error> {
    system.delete_stream(command.stream_id).await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
