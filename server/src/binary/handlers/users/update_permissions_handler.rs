use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::UPDATE_PERMISSIONS_CODE;
use iggy::error::IggyError;
use iggy::users::update_permissions::UpdatePermissions;
use tracing::debug;

pub async fn handle(
    command: &UpdatePermissions,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .update_permissions(session, &command.user_id, command.permissions.clone())
        .await?;
    system
        .metadata
        .apply(
            UPDATE_PERMISSIONS_CODE,
            session.get_user_id(),
            &command.as_bytes(),
            None,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
