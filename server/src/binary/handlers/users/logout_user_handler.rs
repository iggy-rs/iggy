use crate::binary::handlers::users::COMPONENT;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::logout_user::LogoutUser;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_logout_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: LogoutUser,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    system.logout_user(session).await.with_error_context(|_| {
        format!("{COMPONENT} - failed to logout user, session: {session}")
    })?;
    session.clear_user_id();
    sender.send_empty_ok_response().await?;
    Ok(())
}
