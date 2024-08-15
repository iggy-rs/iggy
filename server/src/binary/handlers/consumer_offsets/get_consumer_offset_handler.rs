use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::error::IggyError;
use tracing::debug;

pub async fn handle(
    command: GetConsumerOffset,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let offset = system
        .get_consumer_offset(
            session,
            &command.consumer,
            &command.stream_id,
            &command.topic_id,
            command.partition_id,
        )
        .await;
    if offset.is_err() {
        sender.send_empty_ok_response().await?;
        return Ok(());
    }

    let offset = mapper::map_consumer_offset(&offset?);
    sender.send_ok_response(&offset).await?;
    Ok(())
}
