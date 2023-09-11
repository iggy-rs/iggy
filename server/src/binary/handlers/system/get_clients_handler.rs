use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use iggy::error::Error;
use iggy::system::get_clients::GetClients;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::log::trace;

pub async fn handle(
    command: &GetClients,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    system.permissioner.get_clients(user_context.user_id)?;
    let clients = system.get_clients().await;
    let clients = mapper::map_clients(&clients).await;
    sender.send_ok_response(clients.as_slice()).await?;
    Ok(())
}
