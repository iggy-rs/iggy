use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::error::Error;
use iggy::streams::delete_stream::DeleteStream;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &DeleteStream,
    sender: &mut dyn Sender,
    session: &Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    if !session.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let mut system = system.write().await;
    let stream = system.get_stream(&command.stream_id)?;
    system
        .permissioner
        .delete_stream(session.user_id, stream.stream_id)?;
    system.delete_stream(&command.stream_id).await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
