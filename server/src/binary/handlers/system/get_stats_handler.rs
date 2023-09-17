use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use iggy::error::Error;
use iggy::system::get_stats::GetStats;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &GetStats,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    if !user_context.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let system = system.read().await;
    system.permissioner.get_stats(user_context.user_id)?;
    let stats = system.get_stats().await;
    let bytes = mapper::map_stats(&stats);
    sender.send_ok_response(bytes.as_slice()).await?;
    Ok(())
}
