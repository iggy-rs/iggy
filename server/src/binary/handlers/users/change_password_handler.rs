use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use crate::streaming::utils::crypto;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::CHANGE_PASSWORD_CODE;
use iggy::error::IggyError;
use iggy::users::change_password::ChangePassword;
use tracing::debug;

pub async fn handle(
    command: &ChangePassword,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .change_password(
            session,
            &command.user_id,
            &command.current_password,
            &command.new_password,
        )
        .await?;

    // For the security of the system, we hash the password before storing it in metadata.
    system
        .metadata
        .apply(
            CHANGE_PASSWORD_CODE,
            session.get_user_id(),
            &ChangePassword {
                user_id: command.user_id.to_owned(),
                current_password: "".into(),
                new_password: crypto::hash_password(&command.new_password),
            }
            .as_bytes(),
            None,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
