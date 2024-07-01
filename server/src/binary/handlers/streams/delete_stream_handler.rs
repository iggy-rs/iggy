use crate::binary::sender::Sender;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::streams::delete_stream::DeleteStream;
use tracing::debug;

pub async fn handle(
    command: DeleteStream,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system.delete_stream(session, &command.stream_id).await?;
    system
        .state
        .apply(session.get_user_id(), EntryCommand::DeleteStream(command))
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
