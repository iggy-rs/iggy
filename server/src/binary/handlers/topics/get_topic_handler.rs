use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::get_topic::GetTopic;
use tracing::debug;

pub async fn handle(
    command: &GetTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    let topic = system.find_topic(session, &command.stream_id, &command.topic_id)?;
    let topic = mapper::map_topic(topic).await;
    sender.send_ok_response(&topic).await?;
    Ok(())
}
