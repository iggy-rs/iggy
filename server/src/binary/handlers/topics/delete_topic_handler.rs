use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::topics::delete_topic::DeleteTopic;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &DeleteTopic,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    {
        let system = system.read().await;
        let stream = system.get_stream(&command.stream_id)?;
        let topic = stream.get_topic(&command.topic_id)?;
        system
            .permissioner
            .delete_topic(user_context.user_id, stream.stream_id, topic.topic_id)?;
    }

    let mut system = system.write().await;
    system
        .delete_topic(&command.stream_id, &command.topic_id)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
