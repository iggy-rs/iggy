use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ResultContext;
use iggy::error::IggyError;
use iggy::messages::send_messages::SendMessages;
use tracing::debug;

pub async fn handle(
    command: SendMessages,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let stream_id = command.stream_id.clone();
    let topic_id = command.topic_id.clone();
    let partitioning = command.partitioning.clone();
    let messages = command.messages;
    system
        .append_messages(session, stream_id, topic_id, partitioning, messages)
        .await
        .with_error(|_| {
            format!(
                "Failed to append messages for stream_id: {}, topic_id: {}, partitioning: {}, session: {}",
                command.stream_id, command.topic_id, command.partitioning, session
            )
        })?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
