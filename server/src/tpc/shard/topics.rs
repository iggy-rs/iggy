use crate::streaming::topics::topic::Topic;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::utils::byte_size::IggyByteSize;
use std::borrow::Borrow;

use super::shard::IggyShard;

impl IggyShard {
    pub fn find_topic(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Topic, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .borrow()
            .get_topic(user_id, stream.stream_id, topic.topic_id)?;
        Ok(topic.clone())
    }

    pub fn find_topics(
        &self,
        client_id: u32,
        stream_id: &Identifier,
    ) -> Result<Vec<Topic>, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        self.permissioner
            .borrow()
            .get_topics(user_id, stream.stream_id)?;
        Ok(stream.get_topics())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_topic(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: Option<u32>,
        name: String,
        partitions_count: u32,
        message_expiry: Option<u32>,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: Option<IggyByteSize>,
        replication_factor: Option<u8>,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        self.permissioner
            .borrow()
            .create_topic(user_id, stream.stream_id)?;

        self.get_stream_mut(stream_id)?
            .create_topic(
                topic_id,
                name,
                partitions_count,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor.unwrap_or(1),
            )
            .await?;
        self.metrics.increment_topics(1);
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_topic(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
        message_expiry: Option<u32>,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: Option<IggyByteSize>,
        replication_factor: Option<u8>,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .borrow()
            .update_topic(user_id, stream.stream_id, topic.topic_id)?;

        self.get_stream_mut(stream_id)?
            .update_topic(
                topic_id,
                name,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor.unwrap_or(1),
            )
            .await?;

        // TODO: if message_expiry is changed, we need to check if we need to purge messages based on the new expiry
        // TODO: if max_size_bytes is changed, we need to check if we need to purge messages based on the new size
        // TODO: if replication_factor is changed, we need to do `something`
        Ok(())
    }

    pub async fn delete_topic(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream_id_value;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .borrow()
            .delete_topic(user_id, stream.stream_id, topic.topic_id)?;
        stream_id_value = stream.stream_id;

        let topic = self
            .get_stream_mut(stream_id)?
            .delete_topic(topic_id)
            .await?;

        self.metrics.decrement_topics(1);
        self.metrics
            .decrement_partitions(topic.get_partitions_count());
        self.metrics.decrement_messages(topic.get_messages_count());
        self.metrics
            .decrement_segments(topic.get_segments_count().await);
        self.client_manager
            .borrow_mut()
            .delete_consumer_groups_for_topic(stream_id_value, topic.topic_id);
        Ok(())
    }

    pub async fn purge_topic(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .borrow()
            .purge_topic(user_id, stream.stream_id, topic.topic_id)?;
        topic.purge().await
    }
}
