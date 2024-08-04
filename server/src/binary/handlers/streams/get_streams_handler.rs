use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::streams::get_streams::GetStreams;
use tracing::debug;

pub async fn handle(
    command: GetStreams,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let streams = system.find_streams(session)?;
    let response = mapper::map_streams(&streams);
    sender.send_ok_response(&response).await?;
    Ok(())
}
