use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::streams::purge_stream::PurgeStream;
use tracing::debug;

pub async fn handle(
    command: &PurgeStream,
    sender: &mut impl Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    system.purge_stream(session, &command.stream_id).await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
