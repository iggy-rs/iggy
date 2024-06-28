use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::UPDATE_USER_CODE;
use iggy::error::IggyError;
use iggy::users::update_user::UpdateUser;
use tracing::debug;

pub async fn handle(
    command: &UpdateUser,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .update_user(
            session,
            &command.user_id,
            command.username.clone(),
            command.status,
        )
        .await?;
    system
        .state
        .apply(
            UPDATE_USER_CODE,
            session.get_user_id(),
            &command.to_bytes(),
            None,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
