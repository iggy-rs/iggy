use crate::binary::sender::Sender;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use crate::streaming::utils::crypto;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::users::change_password::ChangePassword;
use tracing::{debug, instrument};

#[instrument(skip_all, fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: ChangePassword,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    {
        let mut system = system.write().await;
        system
            .change_password(
                session,
                &command.user_id,
                &command.current_password,
                &command.new_password,
            )
            .await?;
    }

    // For the security of the system, we hash the password before storing it in metadata.
    let system = system.read().await;
    system
        .state
        .apply(
            session.get_user_id(),
            EntryCommand::ChangePassword(ChangePassword {
                user_id: command.user_id.to_owned(),
                current_password: "".into(),
                new_password: crypto::hash_password(&command.new_password),
            }),
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
