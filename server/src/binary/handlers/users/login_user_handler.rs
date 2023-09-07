use crate::binary::sender::Sender;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::login_user::LoginUser;
use std::sync::Arc;
use streaming::systems::system::System;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &LoginUser,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let system = system.read().await;
    system
        .login_user(&command.username, &command.password)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
