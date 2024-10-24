use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ResultContext;
use iggy::consumer_groups::get_consumer_groups::GetConsumerGroups;
use iggy::error::IggyError;
use tracing::debug;

pub async fn handle(
    command: GetConsumerGroups,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let consumer_groups = system
        .get_consumer_groups(session, &command.stream_id, &command.topic_id)
        .with_error(|_| {
            format!(
                "Failed on getting consumer groups for stream_id: {}, topic_id: {}, session: {}",
                command.stream_id, command.topic_id, session
            )
        })?;
    let consumer_groups = mapper::map_consumer_groups(&consumer_groups).await;
    sender.send_ok_response(&consumer_groups).await?;
    Ok(())
}
