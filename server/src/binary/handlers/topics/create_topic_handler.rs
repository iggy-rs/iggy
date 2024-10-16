use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::topics::create_topic::CreateTopic;
use tracing::{debug, instrument};

#[instrument(skip_all, fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = command.stream_id.as_string()))]
pub async fn handle(
    mut command: CreateTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let response;
    {
        let mut system = system.write().await;
        let topic = system
            .create_topic(
                session,
                &command.stream_id,
                command.topic_id,
                &command.name,
                command.partitions_count,
                command.message_expiry,
                command.compression_algorithm,
                command.max_topic_size,
                command.replication_factor,
            )
            .await?;
        command.message_expiry = topic.message_expiry;
        command.max_topic_size = topic.max_topic_size;
        response = mapper::map_topic(topic).await;
    }

    let system = system.read().await;
    system
        .state
        .apply(session.get_user_id(), EntryCommand::CreateTopic(command))
        .await?;
    sender.send_ok_response(&response).await?;
    Ok(())
}
