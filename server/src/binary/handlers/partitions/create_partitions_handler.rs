use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::CREATE_PARTITIONS_CODE;
use iggy::error::IggyError;
use iggy::partitions::create_partitions::CreatePartitions;
use tracing::debug;

pub async fn handle(
    command: &CreatePartitions,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .create_partitions(
            session,
            &command.stream_id,
            &command.topic_id,
            command.partitions_count,
        )
        .await?;
    system
        .metadata
        .apply(CREATE_PARTITIONS_CODE, &command.as_bytes())
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
