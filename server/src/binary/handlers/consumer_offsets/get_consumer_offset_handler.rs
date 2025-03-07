use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::error::IggyError;
use tracing::debug;

pub async fn handle(
    command: GetConsumerOffset,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let Ok(offset) = system
        .get_consumer_offset(
            session,
            &command.consumer,
            &command.stream_id,
            &command.topic_id,
            command.partition_id,
        )
        .await
    else {
        sender.send_empty_ok_response().await?;
        return Ok(());
    };

    let Some(offset) = offset else {
        sender.send_empty_ok_response().await?;
        return Ok(());
    };

    let offset = mapper::map_consumer_offset(&offset);
    sender.send_ok_response(&offset).await?;
    Ok(())
}
