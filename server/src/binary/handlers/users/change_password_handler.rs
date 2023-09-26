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
    let system = system.read().await;
    system
        .change_password(
            session,
            &command.user_id,
            &command.current_password,
            &command.new_password,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
