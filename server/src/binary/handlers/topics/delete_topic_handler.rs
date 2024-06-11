use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::DELETE_TOPIC_CODE;
use iggy::error::IggyError;
use iggy::topics::delete_topic::DeleteTopic;
use tracing::debug;

pub async fn handle(
    command: &DeleteTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .delete_topic(session, &command.stream_id, &command.topic_id)
        .await?;
    system
        .metadata
        .apply(
            DELETE_TOPIC_CODE,
            session.get_user_id(),
            &command.as_bytes(),
            None,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
