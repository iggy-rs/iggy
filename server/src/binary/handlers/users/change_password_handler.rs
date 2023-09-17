use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::change_password::ChangePassword;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &ChangePassword,
    sender: &mut dyn Sender,
    user_context: &mut UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    if !user_context.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let system = system.read().await;
    let user = system.get_user(&command.user_id).await?;
    if user.id != user_context.user_id {
        system.permissioner.change_password(user_context.user_id)?;
    }
    system
        .change_password(
            &command.user_id,
            &command.current_password,
            &command.new_password,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
