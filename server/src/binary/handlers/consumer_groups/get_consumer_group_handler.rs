use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::error::IggyError;
use tracing::debug;

impl ServerCommandHandler for GetConsumerGroup {
    fn code(&self) -> u32 {
        iggy::command::GET_CONSUMER_GROUP_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let system = system.read().await;
        let Ok(consumer_group) =
            system.get_consumer_group(session, &self.stream_id, &self.topic_id, &self.group_id)
        else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };
        let Some(consumer_group) = consumer_group else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };

        let consumer_group = consumer_group.read().await;
        let consumer_group = mapper::map_consumer_group(&consumer_group).await;
        sender.send_ok_response(&consumer_group).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetConsumerGroup {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetConsumerGroup(get_consumer_group) => Ok(get_consumer_group),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
