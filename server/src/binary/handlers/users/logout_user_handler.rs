use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::users::logout_user::LogoutUser;
use tracing::debug;

pub async fn handle(
    command: &LogoutUser,
    sender: &mut impl Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    system.logout_user(session).await?;
    session.clear_user_id();
    sender.send_empty_ok_response().await?;
    Ok(())
}
