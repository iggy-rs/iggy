use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::error::IggyError;
use tracing::debug;

pub async fn handle(
    command: GetConsumerGroup,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let Ok(consumer_group) = system.get_consumer_group(
        session,
        &command.stream_id,
        &command.topic_id,
        &command.group_id,
    ) else {
        sender.send_empty_ok_response().await?;
        return Ok(());
    };
    let Some(consumer_group) = consumer_group else {
        sender.send_empty_ok_response().await?;
        return Ok(());
    };

    let consumer_group = consumer_group.read().await;
    let consumer_group = mapper::map_consumer_group(&consumer_group).await;
    sender.send_ok_response(&consumer_group).await?;
    Ok(())
}
