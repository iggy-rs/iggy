use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use iggy::error::Error;
use iggy::users::get_users::GetUsers;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::log::debug;

pub async fn handle(
    command: &GetUsers,
    sender: &mut dyn Sender,
    session: &Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    if !session.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let system = system.read().await;
    system.permissioner.get_users(session.user_id)?;
    let users = system.get_users().await?;
    let users = mapper::map_users(&users);
    sender.send_ok_response(users.as_slice()).await?;
    Ok(())
}
