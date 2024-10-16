use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::state::command::EntryCommand;
use crate::state::models::CreatePersonalAccessTokenWithHash;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use tracing::{debug, instrument};

#[instrument(skip_all, fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: CreatePersonalAccessToken,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    let bytes;
    let token_hash;
    debug!("session: {session}, command: {command}");
    {
        let mut system = system.write().await;
        let token = system
            .create_personal_access_token(session, &command.name, command.expiry)
            .await?;
        bytes = mapper::map_raw_pat(&token);
        token_hash = PersonalAccessToken::hash_token(&token);
    }

    let system = system.read().await;
    system
        .state
        .apply(
            session.get_user_id(),
            EntryCommand::CreatePersonalAccessToken(CreatePersonalAccessTokenWithHash {
                command: CreatePersonalAccessToken {
                    name: command.name.to_owned(),
                    expiry: command.expiry,
                },
                hash: token_hash,
            }),
        )
        .await?;
    sender.send_ok_response(&bytes).await?;
    Ok(())
}
