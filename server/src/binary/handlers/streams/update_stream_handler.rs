use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::streams::update_stream::UpdateStream;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &UpdateStream,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let mut system = system.write().await;
    system.permissioner.update_stream(user_context.user_id)?;
    system
        .update_stream(&command.stream_id, &command.name)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
