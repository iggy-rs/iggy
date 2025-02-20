use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::streams::get_stream::GetStream;
use tracing::debug;

pub async fn handle(
    command: GetStream,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let Ok(stream) = system.try_find_stream(session, &command.stream_id) else {
        sender.send_empty_ok_response().await?;
        return Ok(());
    };

    let Some(stream) = stream else {
        sender.send_empty_ok_response().await?;
        return Ok(());
    };

    let response = mapper::map_stream(stream);
    sender.send_ok_response(&response).await?;
    Ok(())
}
