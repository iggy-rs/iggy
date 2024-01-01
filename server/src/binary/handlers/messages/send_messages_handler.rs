use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::messages::send_messages::SendMessages;
use tracing::debug;

pub async fn handle(
    command: &SendMessages,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    system
        .append_messages(
            session,
            &command.stream_id,
            &command.topic_id,
            &command.partitioning,
            &command.messages,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
