use crate::binary::sender::Sender;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &StoreConsumerOffset,
    sender: &mut dyn Sender,
    session: &Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    if !session.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let system = system.read().await;
    let stream = system.get_stream(&command.stream_id)?;
    let topic = stream.get_topic(&command.topic_id)?;
    system
        .permissioner
        .get_consumer_offset(session.user_id, stream.stream_id, topic.topic_id)?;

    let consumer =
        PollingConsumer::from_consumer(&command.consumer, session.client_id, command.partition_id);

    topic
        .store_consumer_offset(consumer, command.offset)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
