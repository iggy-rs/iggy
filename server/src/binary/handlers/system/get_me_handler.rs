use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use crate::streaming::users::user_context::UserContext;
use iggy::error::Error;
use iggy::system::get_me::GetMe;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::log::trace;

pub async fn handle(
    command: &GetMe,
    sender: &mut dyn Sender,
    user_context: &UserContext,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{command}");
    if !user_context.is_authenticated() {
        return Err(Error::Unauthenticated);
    }

    let bytes;
    {
        let system = system.read().await;
        let client = system.get_client(user_context.client_id).await?;
        {
            let client = client.read().await;
            bytes = mapper::map_client(&client).await;
        }
    }
    sender.send_ok_response(bytes.as_slice()).await?;
    Ok(())
}
