use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::purge_topic::PurgeTopic;
use tracing::debug;

pub async fn handle(
    command: &PurgeTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    system
        .purge_topic(session, &command.stream_id, &command.topic_id)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
