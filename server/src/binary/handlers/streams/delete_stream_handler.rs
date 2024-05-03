use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::streams::delete_stream::DeleteStream;
use tracing::debug;

pub async fn handle(
    command: &DeleteStream,
    sender: &mut impl Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system.delete_stream(session, &command.stream_id).await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
