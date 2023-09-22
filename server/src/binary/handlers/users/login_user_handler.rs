use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::login_user::LoginUser;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

pub async fn handle(
    command: &LoginUser,
    sender: &mut dyn Sender,
    session: &mut Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let user = system
        .login_user(
            &command.username,
            &command.password,
            Some(session.client_id),
        )
        .await?;
    if session.is_authenticated() {
        info!(
            "User with ID: {} was already authenticated, removing the previous session...",
            session.user_id
        );
        system
            .logout_user(session.user_id, Some(session.client_id))
            .await?;
    }

    session.set_user_id(user.id);
    let identity_info = mapper::map_identity_info(user.id);
    sender.send_ok_response(identity_info.as_slice()).await?;
    Ok(())
}
