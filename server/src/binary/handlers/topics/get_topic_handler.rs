use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::topics::get_topic::GetTopic;
use tracing::debug;

pub async fn handle(
    command: GetTopic,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let Ok(topic) = system.try_find_topic(session, &command.stream_id, &command.topic_id) else {
        sender.send_empty_ok_response().await?;
        return Ok(());
    };

    let Some(topic) = topic else {
        sender.send_empty_ok_response().await?;
        return Ok(());
    };

    let topic = mapper::map_topic(topic).await;
    sender.send_ok_response(&topic).await?;
    Ok(())
}
