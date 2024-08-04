use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::error::IggyError;
use tracing::debug;

pub async fn handle(
    command: StoreConsumerOffset,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    system
        .store_consumer_offset(
            session,
            command.consumer,
            &command.stream_id,
            &command.topic_id,
            command.partition_id,
            command.offset,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
