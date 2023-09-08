use crate::binary::mapper;
use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::error::Error;
use iggy::messages::poll_messages::PollMessages;
use std::sync::Arc;
use streaming::polling_consumer::PollingConsumer;
use streaming::systems::messages::PollMessagesArgs;
use streaming::systems::system::System;
use streaming::users::user_context::UserContext;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &PollMessages,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let consumer = PollingConsumer::from_consumer(
        &command.consumer,
        user_context.client_id,
        command.partition_id,
    );
    let system = system.read().await;
    let messages = system
        .poll_messages(
            user_context,
            consumer,
            &command.stream_id,
            &command.topic_id,
            PollMessagesArgs {
                strategy: command.strategy,
                count: command.count,
                auto_commit: command.auto_commit,
            },
        )
        .await?;
    let messages = mapper::map_polled_messages(&messages);
    sender.send_ok_response(&messages).await?;
    Ok(())
}
