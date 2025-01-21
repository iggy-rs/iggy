use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::delete_user::DeleteUser;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_delete_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: DeleteUser,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    {
        let mut system = system.write().await;
        system
            .delete_user(session, &command.user_id)
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to delete user with id: {}, session: {session}",
                    command.user_id
                )
            })?;
    }

    let system = system.read().await;
    let user_id = command.user_id.clone();
    system
        .state
        .apply(session.get_user_id(), EntryCommand::DeleteUser(command))
        .await
        .with_error_context(|_| {
            format!(
                "{COMPONENT} - failed to apply delete user with id: {}, session: {session}",
                user_id
            )
        })?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
