use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::update_user::UpdateUser;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &UpdateUser,
    sender: &mut dyn Sender,
    user_context: &mut UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    let system = system.read().await;
    system.permissioner.update_user(user_context.user_id)?;
    system
        .update_user(&command.user_id, command.username.clone(), command.status)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
