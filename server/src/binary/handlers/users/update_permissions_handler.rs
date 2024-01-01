use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::update_permissions::UpdatePermissions;
use tracing::debug;

pub async fn handle(
    command: &UpdatePermissions,
    sender: &mut dyn Sender,
    session: &mut Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .update_permissions(session, &command.user_id, command.permissions.clone())
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
