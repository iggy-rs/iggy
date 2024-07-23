use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use iggy::error::IggyError;
use iggy::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use tracing::debug;

pub async fn handle(
    command: GetPersonalAccessTokens,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let personal_access_tokens = system.get_personal_access_tokens(session).await?;
    let personal_access_tokens = mapper::map_personal_access_tokens(&personal_access_tokens);
    sender.send_ok_response(&personal_access_tokens).await?;
    Ok(())
}
