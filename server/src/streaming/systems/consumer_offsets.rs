use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use iggy::error::Error;
use iggy::identifier::Identifier;
use iggy::models::consumer_offset_info::ConsumerOffsetInfo;

impl System {
    pub async fn store_consumer_offset(
        &self,
        session: &Session,
        consumer: PollingConsumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        offset: u64,
    ) -> Result<(), Error> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner.store_consumer_offset(
            session.user_id,
            stream.stream_id,
            topic.topic_id,
        )?;

        topic.store_consumer_offset(consumer, offset).await
    }

    pub async fn get_consumer_offset(
        &self,
        session: &Session,
        consumer: PollingConsumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<ConsumerOffsetInfo, Error> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .get_consumer_offset(session.user_id, stream.stream_id, topic.topic_id)?;

        topic.get_consumer_offset(consumer).await
    }
}
