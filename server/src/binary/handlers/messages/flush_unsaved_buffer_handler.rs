use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::{handlers::messages::COMPONENT, sender::SenderKind};
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::messages::flush_unsaved_buffer::FlushUnsavedBuffer;
use tracing::{debug, instrument};

impl ServerCommandHandler for FlushUnsavedBuffer {
    fn code(&self) -> u32 {
        iggy::command::FLUSH_UNSAVED_BUFFER_CODE
    }

    #[instrument(skip_all, name = "trace_flush_unsaved_buffer", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string(), iggy_partition_id = self.partition_id, iggy_fsync = self.fsync))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let system = system.read().await;
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let partition_id = self.partition_id;
        let fsync = self.fsync;
        system
            .flush_unsaved_buffer(session, stream_id, topic_id, partition_id, fsync)
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to flush unsaved buffer for stream_id: {}, topic_id: {}, partition_id: {}, session: {}",
                    self.stream_id, self.topic_id, self.partition_id, session
                )
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for FlushUnsavedBuffer {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::FlushUnsavedBuffer(flush_unsaved_buffer) => Ok(flush_unsaved_buffer),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
