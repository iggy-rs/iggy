use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::error::Error;
use iggy::streams::get_stream::GetStream;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &GetStream,
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
    system
        .permissioner
        .get_stream(session.user_id, stream.stream_id)?;
    let stream = mapper::map_stream(stream).await;
    sender.send_ok_response(&stream).await?;
    Ok(())
}
