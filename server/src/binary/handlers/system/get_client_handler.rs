use crate::binary::mapper;
use crate::binary::sender::Sender;
use iggy::error::Error;
use iggy::system::get_client::GetClient;
use std::sync::Arc;
use streaming::system::System;
use tokio::sync::RwLock;
use tracing::log::trace;

pub async fn handle(
    command: &GetClient,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let bytes;
    {
        let system = system.read().await;
        let client = system.get_client(command.client_id).await?;
        {
            let client = client.read().await;
            bytes = mapper::map_client(&client).await;
        }
    }
    sender.send_ok_response(bytes.as_slice()).await?;
    Ok(())
}
