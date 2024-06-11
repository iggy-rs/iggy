use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::DELETE_CONSUMER_GROUP_CODE;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::error::IggyError;
use tracing::debug;

pub async fn handle(
    command: &DeleteConsumerGroup,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .delete_consumer_group(
            session,
            &command.stream_id,
            &command.topic_id,
            &command.group_id,
        )
        .await?;
    system
        .metadata
        .apply(
            DELETE_CONSUMER_GROUP_CODE,
            session.get_user_id(),
            &command.as_bytes(),
            None,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
