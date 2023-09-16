use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::delete_user::DeleteUser;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &DeleteUser,
    sender: &mut dyn Sender,
    user_context: &mut UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    let mut system = system.write().await;
    system.permissioner.delete_user(user_context.user_id)?;
    system.delete_user(&command.user_id).await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
