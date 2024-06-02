use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::PURGE_TOPIC_CODE;
use iggy::error::IggyError;
use iggy::topics::purge_topic::PurgeTopic;
use tracing::debug;

pub async fn handle(
    command: &PurgeTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    system
        .purge_topic(session, &command.stream_id, &command.topic_id)
        .await?;
    system
        .metadata
        .apply(PURGE_TOPIC_CODE, &command.as_bytes())
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
