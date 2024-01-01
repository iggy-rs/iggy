use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::users::login_user::LoginUser;
use tracing::debug;

pub async fn handle(
    command: &LoginUser,
    sender: &mut dyn Sender,
    session: &mut Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    let user = system
        .login_user(&command.username, &command.password, Some(session))
        .await?;
    let identity_info = mapper::map_identity_info(user.id);
    sender.send_ok_response(identity_info.as_slice()).await?;
    Ok(())
}
