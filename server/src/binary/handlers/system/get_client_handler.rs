use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use iggy::system::get_client::GetClient;
use tracing::debug;

pub async fn handle(
    command: GetClient,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let bytes;
    {
        let system = system.read().await;
        let Ok(client) = system.get_client(session, command.client_id).await else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };

        let Some(client) = client else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };

        {
            let client = client.read().await;
            bytes = mapper::map_client(&client);
        }
    }
    sender.send_ok_response(&bytes).await?;
    Ok(())
}
