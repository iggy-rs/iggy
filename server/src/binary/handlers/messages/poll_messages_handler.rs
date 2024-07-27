use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::messages::PollingArgs;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::messages::poll_messages::PollMessages;
use tracing::debug;

pub async fn handle(
    command: PollMessages,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let messages = system
        .poll_messages(
            session,
            &command.consumer,
            &command.stream_id,
            &command.topic_id,
            command.partition_id,
            PollingArgs::new(command.strategy, command.count, command.auto_commit),
        )
        .await?;
    let messages = mapper::map_polled_messages(&messages);
    sender.send_ok_response(&messages).await?;
    Ok(())
}
