use super::shard::IggyShard;
use crate::streaming::polling_consumer::PollingConsumer;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::models::consumer_offset_info::ConsumerOffsetInfo;

impl IggyShard {
    pub async fn store_consumer_offset(
        &self,
        client_id: u32,
        consumer: PollingConsumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        offset: u64,
        persist: bool,
    ) -> Result<(), IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream_lock = self.streams.read().await;
        let stream = self.get_stream(&stream_lock, stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner.borrow().store_consumer_offset(
            user_id,
            stream.stream_id,
            topic.topic_id,
        )?;

        topic.store_consumer_offset(consumer, offset, persist).await
    }

    pub async fn get_consumer_offset(
        &self,
        client_id: u32,
        consumer: PollingConsumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<ConsumerOffsetInfo, IggyError> {
        let user_id = self.ensure_authenticated(client_id)?;
        let stream_lock = self.streams.read().await;
        let stream = self.get_stream(&stream_lock, stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner.borrow().get_consumer_offset(
            user_id,
            stream.stream_id,
            topic.topic_id,
        )?;

        topic.get_consumer_offset(consumer).await
    }
}
