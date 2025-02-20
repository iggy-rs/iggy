use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use iggy::error::IggyError;
use iggy::users::get_user::GetUser;
use tracing::debug;

pub async fn handle(
    command: GetUser,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let Some(user) = system.find_user(session, &command.user_id)? else {
        sender.send_empty_ok_response().await?;
        return Ok(());
    };

    let bytes = mapper::map_user(user);
    sender.send_ok_response(&bytes).await?;
    Ok(())
}
