use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::get_topics::GetTopics;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &GetTopics,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let stream = system.get_stream(&command.stream_id)?;
    system
        .permissioner
        .get_topics(user_context.user_id, stream.stream_id)?;
    let topics = stream.get_topics();
    let topics = mapper::map_topics(&topics).await;
    sender.send_ok_response(&topics).await?;
    Ok(())
}
