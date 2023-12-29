use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use iggy::error::Error;
use iggy::users::get_users::GetUsers;
use tracing::log::debug;

pub async fn handle(
    command: &GetUsers,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    let users = system.get_users(session).await?;
    let users = mapper::map_users(&users);
    sender.send_ok_response(users.as_slice()).await?;
    Ok(())
}
