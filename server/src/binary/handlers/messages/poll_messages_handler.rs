use crate::binary::client_context::ClientContext;
use crate::binary::mapper;
use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::error::Error;
use iggy::messages::poll_messages::PollMessages;
use std::sync::Arc;
use streaming::polling_consumer::PollingConsumer;
use streaming::systems::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &PollMessages,
    sender: &mut dyn Sender,
    client_context: &ClientContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let consumer = PollingConsumer::from_consumer(
        &command.consumer,
        client_context.client_id,
        command.partition_id,
    );
    let system = system.read().await;
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
    let messages = mapper::map_messages(&messages);
    sender.send_ok_response(&messages).await?;
    Ok(())
}
