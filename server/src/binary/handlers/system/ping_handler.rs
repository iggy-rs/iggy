use crate::binary::sender::Sender;
use anyhow::Result;
use shared::error::Error;
use shared::system::ping::Ping;
use tracing::trace;

pub async fn handle(command: Ping, sender: &mut dyn Sender) -> Result<(), Error> {
    trace!("{}", command);
    sender.send_empty_ok_response().await?;
    Ok(())
}
