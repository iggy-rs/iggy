use crate::binary::handlers::users::COMPONENT;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::get_users::GetUsers;
use tracing::debug;

pub async fn handle(
    command: GetUsers,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let users = system
        .get_users(session)
        .await
        .with_error_context(|_| format!("{COMPONENT} - failed to get users, session: {session}"))?;
    let users = mapper::map_users(&users);
    sender.send_ok_response(&users).await?;
    Ok(())
}
