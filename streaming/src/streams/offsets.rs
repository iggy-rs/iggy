use crate::streams::stream::Stream;
use shared::error::Error;

impl Stream {
    pub async fn store_offset(
        &self,
        consumer_id: u32,
        topic_id: u32,
        partition_id: u32,
        offset: u64,
    ) -> Result<(), Error> {
        let topic = self.topics.get(&topic_id);
        if topic.is_none() {
            return Err(Error::TopicNotFound(topic_id, self.id));
        }

        topic
            .unwrap()
            .store_offset(consumer_id, partition_id, offset)
            .await
    }

    pub async fn get_offset(
        &self,
        consumer_id: u32,
        topic_id: u32,
        partition_id: u32,
    ) -> Result<u64, Error> {
        let topic = self.topics.get(&topic_id);
        if topic.is_none() {
            return Err(Error::TopicNotFound(topic_id, self.id));
        }

        topic.unwrap().get_offset(consumer_id, partition_id).await
    }
}
