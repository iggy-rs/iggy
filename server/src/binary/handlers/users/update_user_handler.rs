use crate::binary::sender::Sender;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::users::update_user::UpdateUser;
use tracing::{debug, instrument};

#[instrument(skip_all, fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: UpdateUser,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    {
        let mut system = system.write().await;
        system
            .update_user(
                session,
                &command.user_id,
                command.username.clone(),
                command.status,
            )
            .await?;
    }

    let system = system.read().await;
    system
        .state
        .apply(session.get_user_id(), EntryCommand::UpdateUser(command))
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
