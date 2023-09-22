use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::update_permissions::UpdatePermissions;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &UpdatePermissions,
    sender: &mut dyn Sender,
    session: &mut Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    if !session.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let mut system = system.write().await;
    system.permissioner.update_permissions(session.user_id)?;
    system
        .update_permissions(&command.user_id, command.permissions.clone())
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
