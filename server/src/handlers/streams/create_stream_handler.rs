use crate::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::streams::create_stream::CreateStream;
use streaming::system::System;
use tracing::trace;

pub async fn handle(
    command: CreateStream,
    sender: &mut Sender,
    system: &mut System,
) -> Result<(), Error> {
    trace!("{}", command);
    system
        .create_stream(command.stream_id, &command.name)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
