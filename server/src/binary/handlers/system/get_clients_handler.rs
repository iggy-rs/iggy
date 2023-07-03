use crate::binary::mapper;
use crate::binary::sender::Sender;
use sdk::error::Error;
use sdk::system::get_clients::GetClients;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::log::trace;

pub async fn handle(
    command: GetClients,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    let clients = system.get_clients().await;
    let clients = mapper::map_clients(&clients).await;
    sender.send_ok_response(clients.as_slice()).await?;
    Ok(())
}
