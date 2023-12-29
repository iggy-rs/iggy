use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use iggy::error::Error;
use iggy::users::get_user::GetUser;
use tracing::log::debug;

pub async fn handle(
    command: &GetUser,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    let user = system.find_user(session, &command.user_id).await?;
    let bytes = mapper::map_user(&user);
    sender.send_ok_response(bytes.as_slice()).await?;
    Ok(())
}
