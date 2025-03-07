use crate::binary::mapper;
use crate::binary::{handlers::streams::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::streams::create_stream::CreateStream;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_create_stream", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: CreateStream,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let stream_id = command.stream_id;

    let mut system = system.write().await;
    let stream = system
            .create_stream(session, command.stream_id, &command.name)
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to create stream with id: {:?}, session: {session}",
                    stream_id
                )
            })?;
    let response = mapper::map_stream(stream);

    let system = system.downgrade();
    system
        .state
        .apply(session.get_user_id(), EntryCommand::CreateStream(command))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply create stream for id: {:?}, session: {session}",
                stream_id
            )
        })?;
    sender.send_ok_response(&response).await?;
    Ok(())
}
