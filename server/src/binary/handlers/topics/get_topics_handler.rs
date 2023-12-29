use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::get_topics::GetTopics;
use tracing::debug;

pub async fn handle(
    command: &GetTopics,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    let topics = system.find_topics(session, &command.stream_id)?;
    let topics = mapper::map_topics(&topics).await;
    sender.send_ok_response(&topics).await?;
    Ok(())
}
