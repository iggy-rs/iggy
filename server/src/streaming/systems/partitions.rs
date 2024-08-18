use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use iggy::error::IggyError;
use iggy::identifier::Identifier;

impl System {
    pub async fn create_partitions(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        {
            let topic = self.find_topic(session, stream_id, topic_id)?;
            self.permissioner.create_partitions(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            )?;
        }

        let topic = self.get_stream_mut(stream_id)?.get_topic_mut(topic_id)?;
        topic.add_persisted_partitions(partitions_count).await?;
        topic.reassign_consumer_groups().await;
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);
        Ok(())
    }

    pub async fn delete_partitions(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        {
            let topic = self.find_topic(session, stream_id, topic_id)?;
            self.permissioner.delete_partitions(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            )?;
        }

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
