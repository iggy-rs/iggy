use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::session::Session;
use crate::streaming::systems::messages::PollingArgs;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::messages::poll_messages::PollMessages;
use tracing::debug;

pub async fn handle(
    command: &PollMessages,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let consumer =
        PollingConsumer::from_consumer(&command.consumer, session.client_id, command.partition_id);
    let system = system.read();
    let messages = system
        .poll_messages(
            session,
            consumer,
            &command.stream_id,
            &command.topic_id,
            PollingArgs::new(command.strategy, command.count, command.auto_commit),
        )
        .await?;
    let messages = mapper::map_polled_messages(&messages);
    sender.send_ok_response(&messages).await?;
    Ok(())
}
