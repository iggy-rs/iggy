use crate::binary::sender::Sender;
use sdk::error::Error;
use sdk::system::get_clients::GetClients;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::log::trace;

pub async fn handle(
    command: GetClients,
    _sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let _system = system.read().await;
    // TODO: Implement get clients server handler
    Ok(())
}
