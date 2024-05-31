use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::error::IggyError;
use tracing::debug;

pub async fn handle(
    command: &CreateConsumerGroup,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    let consumer_group = system
        .create_consumer_group(
            session,
            &command.stream_id,
            &command.topic_id,
            command.group_id,
            &command.name,
        )
        .await?;
    let consumer_group = consumer_group.read().await;
    let consumer_group = mapper::map_consumer_group(&consumer_group).await;
    sender.send_ok_response(&consumer_group).await?;
    Ok(())
}
