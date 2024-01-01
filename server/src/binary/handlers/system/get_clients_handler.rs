use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use iggy::error::Error;
use iggy::system::get_clients::GetClients;
use tracing::debug;

pub async fn handle(
    command: &GetClients,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    let clients = system.get_clients(session).await?;
    let clients = mapper::map_clients(&clients).await;
    sender.send_ok_response(clients.as_slice()).await?;
    Ok(())
}
