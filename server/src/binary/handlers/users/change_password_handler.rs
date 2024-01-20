use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
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
    let system = system.read();
    system
        .change_password(
            session,
            &command.user_id,
            &command.current_password,
            &command.new_password,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
