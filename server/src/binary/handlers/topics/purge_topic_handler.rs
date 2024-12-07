use crate::binary::handlers::topics::COMPONENT;
use crate::binary::sender::Sender;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ResultContext;
use iggy::error::IggyError;
use iggy::topics::purge_topic::PurgeTopic;
use tracing::{debug, instrument};

#[instrument(skip_all, fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = command.stream_id.as_string(), iggy_topic_id = command.topic_id.as_string()))]
pub async fn handle(
    command: PurgeTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    system
        .purge_topic(session, &command.stream_id, &command.topic_id)
        .await
        .with_error(|_| {
            format!(
                "{COMPONENT} - failed to purge topic with id: {}, stream_id: {}",
                command.topic_id, command.stream_id
            )
        })?;

    let topic_id = command.topic_id.clone();
    let stream_id = command.stream_id.clone();
    system
        .state
        .apply(session.get_user_id(), EntryCommand::PurgeTopic(command))
        .await
        .with_error(|_| {
            format!(
            "{COMPONENT} - failed to apply purge topic with id: {topic_id}, stream_id: {stream_id}",
        )
        })?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
