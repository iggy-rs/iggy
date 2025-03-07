use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::{handlers::streams::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::streams::create_stream::CreateStream;
use tracing::{debug, instrument};

impl ServerCommandHandler for CreateStream {
    fn code(&self) -> u32 {
        iggy::command::CREATE_STREAM_CODE
    }

    #[instrument(skip_all, name = "trace_create_stream", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let stream_id = self.stream_id;

        let mut system = system.write().await;
        let stream = system
                .create_stream(session, self.stream_id, &self.name)
                .await
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to create stream with id: {:?}, session: {session}",
                        stream_id
                    )
                })?;
        let response = mapper::map_stream(stream);

        let system = system.downgrade();
        system
            .state
            .apply(session.get_user_id(), &EntryCommand::CreateStream(self))
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to apply create stream for id: {:?}, session: {session}",
                    stream_id
                )
            })?;
        sender.send_ok_response(&response).await?;
        Ok(())
    }
}

impl BinaryServerCommand for CreateStream {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::CreateStream(create_stream) => Ok(create_stream),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
