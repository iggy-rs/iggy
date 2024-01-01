use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::error::Error;
use tracing::debug;

pub async fn handle(
    command: &GetConsumerGroup,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    let consumer_group = system.get_consumer_group(
        session,
        &command.stream_id,
        &command.topic_id,
        &command.consumer_group_id,
    )?;
    let consumer_group = consumer_group.read().await;
    let consumer_group = mapper::map_consumer_group(&consumer_group).await;
    sender.send_ok_response(consumer_group.as_slice()).await?;
    Ok(())
}
