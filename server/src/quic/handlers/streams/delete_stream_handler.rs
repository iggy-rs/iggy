use crate::quic::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::streams::delete_stream::DeleteStream;
use streaming::system::System;
use tracing::trace;

pub async fn handle(
    command: DeleteStream,
    sender: &mut Sender,
    system: &mut System,
) -> Result<(), Error> {
    trace!("{}", command);
    system.delete_stream(command.stream_id).await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
