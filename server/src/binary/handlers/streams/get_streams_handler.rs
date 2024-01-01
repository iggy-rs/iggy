use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::streams::get_streams::GetStreams;
use tracing::debug;

pub async fn handle(
    command: &GetStreams,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    let streams = system.find_streams(session)?;
    let streams = mapper::map_streams(&streams).await;
    sender.send_ok_response(streams.as_slice()).await?;
    Ok(())
}
