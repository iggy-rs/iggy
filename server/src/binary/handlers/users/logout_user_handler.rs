use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::logout_user::LogoutUser;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, trace};

pub async fn handle(
    command: &LogoutUser,
    sender: &mut dyn Sender,
    user_context: &mut UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let user_id = user_context.user_id;
    let system = system.read().await;
    system.logout_user(user_id).await?;
    user_context.clear_user_id();
    info!("Cleared user ID: {}", user_id);
    sender.send_empty_ok_response().await?;
    Ok(())
}
