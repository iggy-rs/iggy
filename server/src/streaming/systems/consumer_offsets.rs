use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use crate::streaming::systems::COMPONENT;
use error_set::ErrContext;
use iggy::consumer::Consumer;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::models::consumer_offset_info::ConsumerOffsetInfo;

impl System {
    pub async fn store_consumer_offset(
        &self,
        session: &Session,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let topic = self.find_topic(session, stream_id, topic_id)
            .with_error_context(|_| format!("{COMPONENT} - topic with ID: {topic_id} was not found in stream with ID: {stream_id}"))?;
        self.permissioner.store_consumer_offset(
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id,
        )?;

        topic
            .store_consumer_offset(consumer, offset, partition_id, session.client_id)
            .await
    }

    pub async fn get_consumer_offset(
        &self,
        session: &Session,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        self.ensure_authenticated(session)?;
        let Some(topic) = self.try_find_topic(session, stream_id, topic_id)? else {
            return Ok(None);
        };

        self.permissioner.get_consumer_offset(
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id,
        ).with_error_context(|_| {
            format!(
                "{COMPONENT} - permission denied to get consumer offset for user with ID: {}, consumer: {consumer} in topic with ID: {topic_id} and stream with ID: {stream_id}",
                session.get_user_id(),
            )
        })?;

        topic
            .get_consumer_offset(consumer, partition_id, session.client_id)
            .await
    }

    pub async fn delete_consumer_offset(
        &self,
        session: &Session,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let topic = self.find_topic(session, stream_id, topic_id)
            .with_error_context(|_| format!("{COMPONENT} - topic with ID: {topic_id} was not found in stream with ID: {stream_id}"))?;
        self.permissioner.delete_consumer_offset(
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id,
        ).with_error_context(|_| {
            format!(
                "{COMPONENT} - permission denied to delete consumer offset for user with ID: {}, consumer: {consumer} in topic with ID: {topic_id} and stream with ID: {stream_id}",
                session.get_user_id(),
            )
        })?;

        topic
            .delete_consumer_offset(consumer, partition_id, session.client_id)
            .await
    }
}
