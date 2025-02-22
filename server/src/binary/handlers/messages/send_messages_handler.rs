use crate::binary::handlers::messages::COMPONENT;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use tracing::debug;

pub async fn handle(
    command: iggy::messages::send_messages_server::SendMessages,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    //TODO: Fix me
    /*
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let stream_id = command.stream_id.clone();
    let topic_id = command.topic_id.clone();
    let partitioning = command.partitioning.clone();
    let messages = command.messages;
    // TODO(haze): Add confirmation level after testing is complete
    system
        .append_messages(session, stream_id, topic_id, partitioning, messages, None)
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to append messages for stream_id: {}, topic_id: {}, partitioning: {}, session: {}",
                command.stream_id, command.topic_id, command.partitioning, session
            )
        })?;
    sender.send_empty_ok_response().await?;
    Ok(())
    */
    todo!();
}
