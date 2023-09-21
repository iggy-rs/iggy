use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::logout_user::LogoutUser;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &LogoutUser,
    sender: &mut dyn Sender,
    user_context: &mut UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    if !user_context.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let user_id = user_context.user_id;
    let system = system.read().await;
    system
        .logout_user(user_id, Some(user_context.client_id))
        .await?;
    user_context.clear_user_id();
    sender.send_empty_ok_response().await?;
    Ok(())
}
