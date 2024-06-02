use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::DELETE_USER_CODE;
use iggy::error::IggyError;
use iggy::users::delete_user::DeleteUser;
use tracing::debug;

pub async fn handle(
    command: &DeleteUser,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system.delete_user(session, &command.user_id).await?;
    system
        .metadata
        .apply(DELETE_USER_CODE, &command.as_bytes())
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
