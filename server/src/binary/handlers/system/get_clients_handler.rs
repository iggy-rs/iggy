use crate::binary::handlers::system::COMPONENT;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::system::get_clients::GetClients;
use tracing::debug;

pub async fn handle(
    command: GetClients,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let clients = system.get_clients(session).await.with_error_context(|_| {
        format!("{COMPONENT} - failed to get clients, session: {session}")
    })?;
    let clients = mapper::map_clients(&clients).await;
    sender.send_ok_response(&clients).await?;
    Ok(())
}
