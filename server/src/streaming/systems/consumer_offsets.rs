use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
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
        let topic = self.find_topic(session, stream_id, topic_id)?;
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
    ) -> Result<ConsumerOffsetInfo, IggyError> {
        self.ensure_authenticated(session)?;
        let topic = self.find_topic(session, stream_id, topic_id)?;
        self.permissioner.get_consumer_offset(
            session.get_user_id(),
            topic.stream_id,
            topic.topic_id,
        )?;

        topic
            .get_consumer_offset(consumer, partition_id, session.client_id)
            .await
    }
}
