use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use crate::streaming::topics::topic::Topic;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::locking::IggySharedMutFn;
use iggy::utils::byte_size::IggyByteSize;

impl System {
    pub fn find_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<&Topic, IggyError> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .get_topic(session.get_user_id(), stream.stream_id, topic.topic_id)?;
        Ok(topic)
    }

    pub fn find_topics(
        &self,
        session: &Session,
        stream_id: &Identifier,
    ) -> Result<Vec<&Topic>, IggyError> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(stream_id)?;
        self.permissioner
            .get_topics(session.get_user_id(), stream.stream_id)?;
        Ok(stream.get_topics())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_topic(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: Option<u32>,
        name: &str,
        partitions_count: u32,
        message_expiry: Option<u32>,
        max_topic_size: Option<IggyByteSize>,
        replication_factor: u8,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id)?;
            self.permissioner
                .create_topic(session.get_user_id(), stream.stream_id)?;
        }

        self.get_stream_mut(stream_id)?
            .create_topic(
                topic_id,
                name,
                partitions_count,
                message_expiry,
                max_topic_size,
                replication_factor,
            )
            .await?;
        self.metrics.increment_topics(1);
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_topic(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        message_expiry: Option<u32>,
        max_topic_size: Option<IggyByteSize>,
        replication_factor: u8,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id)?;
            let topic = stream.get_topic(topic_id)?;
            self.permissioner.update_topic(
                session.get_user_id(),
                stream.stream_id,
                topic.topic_id,
            )?;
        }

        self.get_stream_mut(stream_id)?
            .update_topic(
                topic_id,
                name,
                message_expiry,
                max_topic_size,
                replication_factor,
            )
            .await?;

        // TODO: if message_expiry is changed, we need to check if we need to purge messages based on the new expiry
        // TODO: if max_size_bytes is changed, we need to check if we need to purge messages based on the new size
        // TODO: if replication_factor is changed, we need to do `something`
        Ok(())
    }

    pub async fn delete_topic(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let stream_id_value;
        {
            let stream = self.get_stream(stream_id)?;
            let topic = stream.get_topic(topic_id)?;
            self.permissioner.delete_topic(
                session.get_user_id(),
                stream.stream_id,
                topic.topic_id,
            )?;
            stream_id_value = stream.stream_id;
        }

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
        let client_manager = self.client_manager.read().await;
        client_manager
            .delete_consumer_groups_for_topic(stream_id_value, topic.topic_id)
            .await;
        Ok(())
    }

    pub async fn purge_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .purge_topic(session.get_user_id(), stream.stream_id, topic.topic_id)?;
        topic.purge().await
    }
}
