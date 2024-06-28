use crate::binary::sender::Sender;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use tracing::debug;

pub async fn handle(
    command: DeletePersonalAccessToken,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .delete_personal_access_token(session, &command.name)
        .await?;
    system
        .state
        .apply(
            session.get_user_id(),
            EntryCommand::DeletePersonalAccessToken(command),
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
