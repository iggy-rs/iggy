use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use iggy::error::Error;
use iggy::users::get_user::GetUser;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::log::debug;

pub async fn handle(
    command: &GetUser,
    sender: &mut dyn Sender,
    session: &Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    if !session.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let system = system.read().await;
    let user = system.get_user(&command.user_id).await?;
    if user.id != session.user_id {
        system.permissioner.get_user(session.user_id)?;
    }
    let bytes = mapper::map_user(&user);
    sender.send_ok_response(bytes.as_slice()).await?;
    Ok(())
}
