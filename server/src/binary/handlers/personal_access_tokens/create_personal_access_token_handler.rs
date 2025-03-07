use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::{handlers::personal_access_tokens::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::state::models::CreatePersonalAccessTokenWithHash;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use tracing::{debug, instrument};

impl ServerCommandHandler for CreatePersonalAccessToken {
    fn code(&self) -> u32 {
        iggy::command::CREATE_PERSONAL_ACCESS_TOKEN_CODE
    }

    #[instrument(skip_all, name = "trace_create_personal_access_token", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let mut system = system.write().await;
        let token = system
                .create_personal_access_token(session, &self.name, self.expiry)
                .await
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to create personal access token with name: {}, session: {session}",
                        self.name
                    )
                })?;
        let bytes = mapper::map_raw_pat(&token);
        let token_hash = PersonalAccessToken::hash_token(&token);

        let system = system.downgrade();
        system
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::CreatePersonalAccessToken(CreatePersonalAccessTokenWithHash {
                    command: CreatePersonalAccessToken {
                        name: self.name.to_owned(),
                        expiry: self.expiry,
                    },
                    hash: token_hash,
                }),
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to create personal access token with name: {}, session: {session}",
                    self.name
                )
            })?;
        sender.send_ok_response(&bytes).await?;
        Ok(())
    }
}

impl BinaryServerCommand for CreatePersonalAccessToken {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::CreatePersonalAccessToken(create_personal_access_token) => {
                Ok(create_personal_access_token)
            }
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
