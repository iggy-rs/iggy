use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use iggy::error::Error;
use iggy::system::get_stats::GetStats;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &GetStats,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let stats = system.get_stats().await;
    let bytes = mapper::map_stats(&stats);
    sender.send_ok_response(bytes.as_slice()).await?;
    Ok(())
}
