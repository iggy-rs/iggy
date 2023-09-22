use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use iggy::error::Error;
use iggy::system::get_client::GetClient;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

pub async fn handle(
    command: &GetClient,
    sender: &mut dyn Sender,
    session: &Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    if !session.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let bytes;
    {
        let system = system.read().await;
        system.permissioner.get_client(session.user_id)?;
        let client = system.get_client(command.client_id).await?;
        {
            let client = client.read().await;
            bytes = mapper::map_client(&client).await;
        }
    }
    sender.send_ok_response(bytes.as_slice()).await?;
    Ok(())
}
