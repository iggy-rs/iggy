use crate::sender::Sender;
use anyhow::Result;
use shared::error::Error;

pub async fn handle(sender: &mut Sender) -> Result<(), Error> {
    sender.send_empty_ok_response().await?;
    Ok(())
}
