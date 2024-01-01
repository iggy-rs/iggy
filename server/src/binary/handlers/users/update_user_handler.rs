use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::update_user::UpdateUser;
use tracing::debug;

pub async fn handle(
    command: &UpdateUser,
    sender: &mut dyn Sender,
    session: &mut Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    system
        .update_user(
            session,
            &command.user_id,
            command.username.clone(),
            command.status,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
