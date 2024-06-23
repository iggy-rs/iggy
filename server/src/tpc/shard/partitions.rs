use crate::streaming::session::Session;
use iggy::error::IggyError;
use iggy::identifier::Identifier;

use super::shard::IggyShard;

impl IggyShard {
    pub async fn create_partitions(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .borrow()
            .create_partitions(user_id, stream.stream_id, topic.topic_id)?;

        let topic = self.get_stream_mut(stream_id)?.get_topic_mut(topic_id)?;
        topic.add_persisted_partitions(partitions_count).await?;
        topic.reassign_consumer_groups().await;
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);
        Ok(())
    }

    pub async fn delete_partitions(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .borrow()
            .delete_partitions(user_id, stream.stream_id, topic.topic_id)?;

        let topic = self.get_stream_mut(stream_id)?.get_topic_mut(topic_id)?;
        let partitions = topic.delete_persisted_partitions(partitions_count).await?;
        topic.reassign_consumer_groups().await;
        if let Some(partitions) = partitions {
            self.metrics.decrement_partitions(partitions_count);
            self.metrics.decrement_segments(partitions.segments_count);
            self.metrics.decrement_messages(partitions.messages_count);
        }
        Ok(())
    }
}
