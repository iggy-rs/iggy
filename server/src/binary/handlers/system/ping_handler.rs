use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::system::ping::Ping;
use tracing::debug;

pub async fn handle(
    command: Ping,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    // TODO: Update client heartbeat
    sender.send_empty_ok_response().await?;
    Ok(())
}
