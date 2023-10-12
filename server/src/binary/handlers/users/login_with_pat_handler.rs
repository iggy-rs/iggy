use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::login_pat::LoginWithPersonalAccessToken;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &LoginWithPersonalAccessToken,
    sender: &mut dyn Sender,
    session: &mut Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let user = system
        .login_with_personal_access_token(&command.token, Some(session))
        .await?;
    let identity_info = mapper::map_identity_info(user.id);
    sender.send_ok_response(identity_info.as_slice()).await?;
    Ok(())
}
