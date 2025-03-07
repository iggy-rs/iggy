use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::{handlers::topics::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::topics::create_topic::CreateTopic;
use tracing::{debug, instrument};

impl ServerCommandHandler for CreateTopic {
    fn code(&self) -> u32 {
        iggy::command::CREATE_TOPIC_CODE
    }

    #[instrument(skip_all, name = "trace_create_topic", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string()))]
    async fn handle(
        mut self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id;
        let mut system = system.write().await;
        let topic = system
                .create_topic(
                    session,
                    &self.stream_id,
                    self.topic_id,
                    &self.name,
                    self.partitions_count,
                    self.message_expiry,
                    self.compression_algorithm,
                    self.max_topic_size,
                    self.replication_factor,
                )
                .await
                .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to create topic for stream_id: {stream_id}, topic_id: {:?}",
                    topic_id
                ))?;
        self.message_expiry = topic.message_expiry;
        self.max_topic_size = topic.max_topic_size;
        let response = mapper::map_topic(topic).await;

        let system = system.downgrade();
        system
            .state
            .apply(session.get_user_id(),&EntryCommand::CreateTopic(self))
            .await
            .with_error_context(|error| {
                format!(
                "{COMPONENT} (error: {error}) - failed to apply create topic for stream_id: {stream_id}, topic_id: {:?}",
                topic_id
            )
            })?;
        sender.send_ok_response(&response).await?;
        Ok(())
    }
}

impl BinaryServerCommand for CreateTopic {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::CreateTopic(create_topic) => Ok(create_topic),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
