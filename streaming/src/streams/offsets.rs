use crate::streams::stream::Stream;
use shared::error::Error;

impl Stream {
    pub async fn store_offset(
        &mut self,
        consumer_id: u32,
        topic_id: u32,
        partition_id: u32,
        offset: u64,
    ) -> Result<(), Error> {
        let topic = self.topics.get_mut(&topic_id);
        if topic.is_none() {
            return Err(Error::TopicNotFound(topic_id));
        }

        topic
            .unwrap()
            .store_offset(consumer_id, partition_id, offset)
            .await
    }
}
