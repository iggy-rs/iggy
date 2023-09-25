use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::delete_user::DeleteUser;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &DeleteUser,
    sender: &mut dyn Sender,
    session: &mut Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write().await;
    system.delete_user(session, &command.user_id).await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
