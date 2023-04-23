use crate::handlers::STATUS_OK;
use anyhow::Result;
use shared::error::Error;
use shared::streams::delete_stream::DeleteStream;
use streaming::system::System;

pub async fn handle(
    command: DeleteStream,
    send: &mut quinn::SendStream,
    system: &mut System,
) -> Result<(), Error> {
    system.delete_stream(command.stream_id).await?;
    send.write_all(STATUS_OK).await?;
    Ok(())
}
