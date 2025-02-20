use crate::binary::mapper;
use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::login_user::LoginUser;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_login_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: LoginUser,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let user = system
        .login_user(&command.username, &command.password, Some(session))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to login user with name: {}, session: {session}",
                command.username
            )
        })?;
    let identity_info = mapper::map_identity_info(user.id);
    sender.send_ok_response(&identity_info).await?;
    Ok(())
}
