use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use iggy::error::Error;
use iggy::users::get_pats::GetPersonalAccessTokens;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::log::debug;

pub async fn handle(
    command: &GetPersonalAccessTokens,
    sender: &mut dyn Sender,
    session: &Session,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let pats = system.get_personal_access_tokens(session).await?;
    let pats = mapper::map_pats(&pats);
    sender.send_ok_response(pats.as_slice()).await?;
    Ok(())
}
