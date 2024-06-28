use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::CREATE_TOPIC_CODE;
use iggy::error::IggyError;
use iggy::topics::create_topic::CreateTopic;
use tracing::debug;

pub async fn handle(
    command: &CreateTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
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
    system
        .state
        .apply(
            CREATE_TOPIC_CODE,
            session.get_user_id(),
            &command.to_bytes(),
            None,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
