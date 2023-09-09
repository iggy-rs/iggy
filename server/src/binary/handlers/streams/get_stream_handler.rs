use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::streams::get_stream::GetStream;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &GetStream,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let stream = system.get_stream(&command.stream_id)?;
    system
        .permissioner
        .get_stream(user_context.user_id, stream.stream_id)?;
    let stream = mapper::map_stream(stream).await;
    sender.send_ok_response(&stream).await?;
    Ok(())
}
