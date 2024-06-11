use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use crate::streaming::utils::crypto;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::CREATE_USER_CODE;
use iggy::error::IggyError;
use iggy::users::create_user::CreateUser;
use tracing::debug;

pub async fn handle(
    command: &CreateUser,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .create_user(
            session,
            &command.username,
            &command.password,
            command.status,
            command.permissions.clone(),
        )
        .await?;

    // For the security of the system, we hash the password before storing it in metadata.
    let command = CreateUser {
        username: command.username.to_owned(),
        password: crypto::hash_password(&command.password),
        status: command.status,
        permissions: command.permissions.clone(),
    };
    system
        .metadata
        .apply(
            CREATE_USER_CODE,
            session.get_user_id(),
            &command.as_bytes(),
            None,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
