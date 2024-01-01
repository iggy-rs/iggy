use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::update_topic::UpdateTopic;
use tracing::debug;

pub async fn handle(
    command: &UpdateTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .update_topic(
            session,
            &command.stream_id,
            &command.topic_id,
            &command.name,
            command.message_expiry,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
