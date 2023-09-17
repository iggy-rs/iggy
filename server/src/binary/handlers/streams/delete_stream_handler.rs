use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::streams::delete_stream::DeleteStream;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &DeleteStream,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    if !user_context.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let mut system = system.write().await;
    let stream = system.get_stream(&command.stream_id)?;
    system
        .permissioner
        .delete_stream(user_context.user_id, stream.stream_id)?;
    system.delete_stream(&command.stream_id).await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
