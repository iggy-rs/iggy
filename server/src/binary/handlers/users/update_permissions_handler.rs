use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::update_permissions::UpdatePermissions;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &UpdatePermissions,
    sender: &mut dyn Sender,
    user_context: &mut UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    if !user_context.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let mut system = system.write().await;
    system
        .permissioner
        .update_permissions(user_context.user_id)?;
    system
        .update_permissions(&command.user_id, command.permissions.clone())
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
