use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use tracing::debug;

pub async fn handle(
    command: &LoginWithPersonalAccessToken,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    let user = system
        .login_with_personal_access_token(&command.token, Some(session))
        .await?;
    let identity_info = mapper::map_identity_info(user.id);
    sender.send_ok_response(&identity_info).await?;
    Ok(())
}
