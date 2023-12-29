use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::Error;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use tracing::debug;

pub async fn handle(
    command: &CreatePersonalAccessToken,
    sender: &mut dyn Sender,
    session: &mut Session,
    system: &SharedSystem,
) -> Result<(), Error> {
    debug!("session: {session}, command: {command}");
    let system = system.read();
    let token = system
        .create_personal_access_token(session, &command.name, command.expiry)
        .await?;
    let bytes = mapper::map_raw_pat(&token);
    sender.send_ok_response(bytes.as_slice()).await?;
    Ok(())
}
