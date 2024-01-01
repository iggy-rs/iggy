use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::delete_user::DeleteUser;
use tracing::debug;

pub async fn handle(
    command: &DeleteUser,
    sender: &mut dyn Sender,
    session: &mut Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system.delete_user(session, &command.user_id).await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
