use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::create_topic::CreateTopic;
use tracing::debug;

pub async fn handle(
    command: &CreateTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .create_topic(
            session,
            &command.stream_id,
            command.topic_id,
            &command.name,
            command.partitions_count,
            command.message_expiry,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
