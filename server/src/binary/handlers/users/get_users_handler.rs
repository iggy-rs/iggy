use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use iggy::error::Error;
use iggy::users::get_users::GetUsers;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::log::trace;

pub async fn handle(
    command: &GetUsers,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    let system = system.read().await;
    system.permissioner.get_users(user_context.user_id)?;
    let users = system.get_users().await?;
    let users = mapper::map_users(&users);
    sender.send_ok_response(users.as_slice()).await?;
    Ok(())
}
