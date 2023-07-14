use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::error::Error;
use iggy::system::ping::Ping;
use tracing::trace;

pub async fn handle(command: &Ping, sender: &mut dyn Sender) -> Result<(), Error> {
    trace!("{}", command);
    sender.send_empty_ok_response().await?;
    Ok(())
}
