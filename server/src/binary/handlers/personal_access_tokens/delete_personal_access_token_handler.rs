use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::{handlers::personal_access_tokens::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use tracing::{debug, instrument};

impl ServerCommandHandler for DeletePersonalAccessToken {
    fn code(&self) -> u32 {
        iggy::command::DELETE_PERSONAL_ACCESS_TOKEN_CODE
    }

    #[instrument(skip_all, name = "trace_delete_personal_access_token", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let token_name = self.name.clone();

        let mut system = system.write().await;
        system
                .delete_personal_access_token(session, &self.name)
                .await
                .with_error_context(|error| {format!(
                    "{COMPONENT} (error: {error}) - failed to delete personal access token with name: {token_name}, session: {session}"
                )})?;

        let system = system.downgrade();
        system
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::DeletePersonalAccessToken(DeletePersonalAccessToken {
                    name: self.name,
                }),
            )
            .await
            .with_error_context(|error| {format!(
                "{COMPONENT} (error: {error}) - failed to apply delete personal access token with name: {token_name}, session: {session}"
            )})?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for DeletePersonalAccessToken {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeletePersonalAccessToken(delete_personal_access_token) => {
                Ok(delete_personal_access_token)
            }
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
