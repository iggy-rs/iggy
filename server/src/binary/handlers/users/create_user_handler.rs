use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};
use crate::binary::mapper;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use crate::streaming::utils::crypto;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::create_user::CreateUser;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_create_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: CreateUser,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let response;
    {
        let mut system = system.write().await;
        let user = system
            .create_user(
                session,
                &command.username,
                &command.password,
                command.status,
                command.permissions.clone(),
            )
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to create user with name: {}, session: {session}",
                    command.username
                )
            })?;
        response = mapper::map_user(user);
    }

    // For the security of the system, we hash the password before storing it in metadata.
    let system = system.read().await;
    system
        .state
        .apply(
            session.get_user_id(),
            EntryCommand::CreateUser(CreateUser {
                username: command.username.to_owned(),
                password: crypto::hash_password(&command.password),
                status: command.status,
                permissions: command.permissions.clone(),
            }),
        )
        .await
        .with_error_context(|_| {
            format!(
                "{COMPONENT} - failed to apply create user with name: {}, session: {session}",
                command.username
            )
        })?;
    sender.send_ok_response(&response).await?;
    Ok(())
}
