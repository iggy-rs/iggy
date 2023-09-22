use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::change_password::ChangePassword;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &ChangePassword,
    sender: &mut dyn Sender,
    session: &mut Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    if !session.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let system = system.read().await;
    let user = system.get_user(&command.user_id).await?;
    if user.id != session.user_id {
        system.permissioner.change_password(session.user_id)?;
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
