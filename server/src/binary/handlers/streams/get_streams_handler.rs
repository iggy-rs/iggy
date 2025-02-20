use crate::binary::handlers::streams::COMPONENT;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::streams::get_streams::GetStreams;
use tracing::debug;

pub async fn handle(
    command: GetStreams,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let streams = system.find_streams(session).with_error_context(|error| {
        format!("{COMPONENT} (error: {error}) - failed to find streams for session: {session}")
    })?;
    let response = mapper::map_streams(&streams);
    sender.send_ok_response(&response).await?;
    Ok(())
}
