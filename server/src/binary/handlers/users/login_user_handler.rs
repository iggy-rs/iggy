use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::login_user::LoginUser;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, trace};

pub async fn handle(
    command: &LoginUser,
    sender: &mut dyn Sender,
    user_context: &mut UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    let system = system.read().await;
    let user = system
        .login_user(
            &command.username,
            &command.password,
            Some(user_context.client_id),
        )
        .await?;
    user_context.set_user_id(user.id);
    info!("Set user ID: {}", user.id);
    let identity_info = mapper::map_identity_info(user.id);
    sender.send_ok_response(identity_info.as_slice()).await?;
    Ok(())
}
