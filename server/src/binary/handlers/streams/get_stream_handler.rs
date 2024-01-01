use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::streams::get_stream::GetStream;
use tracing::debug;

pub async fn handle(
    command: &GetStream,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    let stream = system.find_stream(session, &command.stream_id)?;
    let stream = mapper::map_stream(stream).await;
    sender.send_ok_response(&stream).await?;
    Ok(())
}
