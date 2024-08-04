use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::topics::get_topics::GetTopics;
use tracing::debug;

pub async fn handle(
    command: GetTopics,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let topics = system.find_topics(session, &command.stream_id)?;
    let response = mapper::map_topics(&topics);
    sender.send_ok_response(&response).await?;
    Ok(())
}
