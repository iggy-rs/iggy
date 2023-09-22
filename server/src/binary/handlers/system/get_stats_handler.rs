use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use iggy::error::Error;
use iggy::system::get_stats::GetStats;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &GetStats,
    sender: &mut dyn Sender,
    session: &Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    if !session.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let system = system.read().await;
    system.permissioner.get_stats(session.user_id)?;
    let stats = system.get_stats().await;
    let bytes = mapper::map_stats(&stats);
    sender.send_ok_response(bytes.as_slice()).await?;
    Ok(())
}
