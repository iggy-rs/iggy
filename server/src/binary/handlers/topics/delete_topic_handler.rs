use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::delete_topic::DeleteTopic;
use tracing::debug;

pub async fn handle(
    command: &DeleteTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .delete_topic(session, &command.stream_id, &command.topic_id)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
