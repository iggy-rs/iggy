use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::CREATE_PERSONAL_ACCESS_TOKEN_CODE;
use iggy::error::IggyError;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use tracing::debug;

pub async fn handle(
    command: &CreatePersonalAccessToken,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    let token = system
        .create_personal_access_token(session, &command.name, command.expiry)
        .await?;
    let bytes = mapper::map_raw_pat(&token);
    let token_hash = PersonalAccessToken::hash_token(&token);
    system
        .metadata
        .apply(
            CREATE_PERSONAL_ACCESS_TOKEN_CODE,
            session.get_user_id(),
            &command.as_bytes(),
            Some(token_hash.as_bytes()),
        )
        .await?;
    sender.send_ok_response(&bytes).await?;
    Ok(())
}
