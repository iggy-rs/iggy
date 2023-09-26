use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &GetConsumerOffset,
    sender: &mut dyn Sender,
    session: &Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let consumer =
        PollingConsumer::from_consumer(&command.consumer, session.client_id, command.partition_id);
    let offset = system
        .get_consumer_offset(session, consumer, &command.stream_id, &command.topic_id)
        .await?;
    let offset = mapper::map_consumer_offset(&offset);
    sender.send_ok_response(&offset).await?;
    Ok(())
}
