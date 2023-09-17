use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use iggy::error::Error;
use iggy::users::get_user::GetUser;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::log::trace;

pub async fn handle(
    command: &GetUser,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    // TODO: Validate authenticated user in all the required handlers.
    if !user_context.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let system = system.read().await;
    let user = system.get_user(&command.user_id).await?;
    if user.id != user_context.user_id {
        system.permissioner.get_user(user_context.user_id)?;
    }
    let bytes = mapper::map_user(&user);
    sender.send_ok_response(bytes.as_slice()).await?;
    Ok(())
}
