use crate::handlers::STATUS_OK;
use anyhow::Result;
use shared::error::Error;
use shared::streams::create_stream::CreateStream;
use streaming::system::System;

pub async fn handle(
    command: CreateStream,
    send: &mut quinn::SendStream,
    system: &mut System,
) -> Result<(), Error> {
    system
        .create_stream(command.stream_id, &command.name)
        .await?;
    send.write_all(STATUS_OK).await?;
    Ok(())
}
