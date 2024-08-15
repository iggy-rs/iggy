use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::streams::get_stream::GetStream;
use tracing::debug;

pub async fn handle(
    command: GetStream,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let stream = system.find_stream(session, &command.stream_id);
    if stream.is_err() {
        sender.send_empty_ok_response().await?;
        return Ok(());
    }

    let response = mapper::map_stream(stream?);
    sender.send_ok_response(&response).await?;
    Ok(())
}
