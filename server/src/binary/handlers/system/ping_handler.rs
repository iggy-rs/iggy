use crate::binary::sender::Sender;
use anyhow::Result;
use sdk::error::Error;
use sdk::system::ping::Ping;
use tracing::trace;

pub async fn handle(command: &Ping, sender: &mut dyn Sender) -> Result<(), Error> {
    trace!("{}", command);
    sender.send_empty_ok_response().await?;
    Ok(())
}
