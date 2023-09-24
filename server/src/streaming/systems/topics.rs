use crate::streaming::systems::system::System;
use iggy::error::Error;
use iggy::identifier::Identifier;

impl System {
    pub async fn create_topic(
        &mut self,
        stream_id: &Identifier,
        topic_id: u32,
        name: &str,
        partitions_count: u32,
        message_expiry: Option<u32>,
    ) -> Result<(), Error> {
        self.get_stream_mut(stream_id)?
            .create_topic(topic_id, name, partitions_count, message_expiry)
            .await?;
        self.metrics.increment_topics(1);
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);
        Ok(())
    }

    pub async fn delete_topic(
        &mut self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), Error> {
        let stream_id_value;
        {
            let stream = self.get_stream(stream_id)?;
            stream_id_value = stream.stream_id;
        }

        let topic = self
            .get_stream_mut(stream_id)?
            .delete_topic(topic_id)
            .await?;
        self.metrics.decrement_topics(1);
        self.metrics
            .decrement_partitions(topic.get_partitions_count());
        self.metrics
            .decrement_messages(topic.get_messages_count().await);
        self.metrics
            .decrement_segments(topic.get_segments_count().await);
        let client_manager = self.client_manager.read().await;
        client_manager
            .delete_consumer_groups_for_topic(stream_id_value, topic.topic_id)
            .await;
        Ok(())
    }
}
