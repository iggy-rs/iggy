use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::streams::create_stream::CreateStream;
use tracing::debug;

pub async fn handle(
    command: &CreateStream,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .create_stream(session, command.stream_id, &command.name)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
