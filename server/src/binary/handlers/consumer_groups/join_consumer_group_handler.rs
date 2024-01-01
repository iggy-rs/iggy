use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::consumer_groups::join_consumer_group::JoinConsumerGroup;
use iggy::error::Error;
use tracing::debug;

pub async fn handle(
    command: &JoinConsumerGroup,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    system
        .join_consumer_group(
            session,
            &command.stream_id,
            &command.topic_id,
            &command.consumer_group_id,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
