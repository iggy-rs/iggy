use crate::binary::{handlers::streams::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::streams::purge_stream::PurgeStream;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_purge_stream", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = command.stream_id.as_string()))]
pub async fn handle(
    command: PurgeStream,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let stream_id = command.stream_id.clone();

    system
        .purge_stream(session, &command.stream_id)
        .await
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to purge stream with id: {stream_id}, session: {session}")
        })?;

    system
        .state
        .apply(session.get_user_id(), EntryCommand::PurgeStream(command))
        .await
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to apply purge stream with id: {stream_id}, session: {session}")
        })?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
