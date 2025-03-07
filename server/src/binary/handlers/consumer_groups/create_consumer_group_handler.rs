use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::{handlers::consumer_groups::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::error::IggyError;
use tracing::{debug, instrument};

impl ServerCommandHandler for CreateConsumerGroup {
    fn code(&self) -> u32 {
        iggy::command::CREATE_CONSUMER_GROUP_CODE
    }

    #[instrument(skip_all, name = "trace_create_consumer_group", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let mut system = system.write().await;
        let consumer_group = system
                .create_consumer_group(
                    session,
                    &self.stream_id,
                    &self.topic_id,
                    self.group_id,
                    &self.name,
                )
                .await
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to create consumer group for stream_id: {}, topic_id: {}, group_id: {:?}, session: {session}",
                        self.stream_id, self.topic_id, self.group_id
                    )
                })?;
        let consumer_group = consumer_group.read().await;
        let response = mapper::map_consumer_group(&consumer_group).await;
        drop(consumer_group);

        let system = system.downgrade();
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let group_id = self.group_id;

        system
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::CreateConsumerGroup(self),
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to apply create consumer group for stream_id: {}, topic_id: {}, group_id: {:?}, session: {}",
                    stream_id, topic_id, group_id, session
                )
            })?;
        sender.send_ok_response(&response).await?;
        Ok(())
    }
}

impl BinaryServerCommand for CreateConsumerGroup {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::CreateConsumerGroup(create_consumer_group) => Ok(create_consumer_group),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
