use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::create_user::CreateUser;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &CreateUser,
    sender: &mut dyn Sender,
    user_context: &mut UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    if !user_context.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let system = system.read().await;
    system.permissioner.create_user(user_context.user_id)?;
    system
        .create_user(
            &command.username,
            &command.password,
            command.permissions.clone(),
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
