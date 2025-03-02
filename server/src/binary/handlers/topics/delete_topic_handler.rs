use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::{handlers::topics::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::topics::delete_topic::DeleteTopic;
use tracing::{debug, instrument};

impl ServerCommandHandler for DeleteTopic {
    fn code(&self) -> u32 {
        iggy::command::DELETE_TOPIC_CODE
    }

    #[instrument(skip_all, name = "trace_delete_topic", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();

        let mut system = system.write().await;
        system
                .delete_topic(session, &self.stream_id, &self.topic_id)
                .await
                .with_error_context(|error| format!(
                    "{COMPONENT} (error: {error}) - failed to delete topic with ID: {topic_id} in stream with ID: {stream_id}, session: {session}",
                ))?;

        let system = system.downgrade();
        system
            .state
            .apply(session.get_user_id(), &EntryCommand::DeleteTopic(self))
            .await
            .with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - failed to apply delete topic with ID: {topic_id} in stream with ID: {stream_id}, session: {session}",
            ))?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for DeleteTopic {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeleteTopic(delete_topic) => Ok(delete_topic),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
