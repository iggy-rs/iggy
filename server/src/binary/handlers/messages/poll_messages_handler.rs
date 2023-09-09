use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::messages::poll_messages::PollMessages;
use std::sync::Arc;
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
    let stream = system.get_stream(&command.stream_id)?;
    let topic = stream.get_topic(&command.topic_id)?;
    system
        .permissioner
        .poll_messages(user_context.user_id, stream.stream_id, topic.topic_id)?;

    let messages = system
        .poll_messages(
            consumer,
            &command.stream_id,
            &command.topic_id,
            command.strategy,
            command.count,
            command.auto_commit,
        )
        .await?;
    let messages = mapper::map_polled_messages(&messages);
    sender.send_ok_response(&messages).await?;
    Ok(())
}
