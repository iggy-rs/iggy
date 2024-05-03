use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use tracing::debug;

pub async fn handle(
    command: &DeletePersonalAccessToken,
    sender: &mut impl Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    system
        .delete_personal_access_token(session, &command.name)
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
