use crate::message::Message;
use crate::streams::stream::Stream;
use shared::error::Error;
use std::sync::Arc;

impl Stream {
    pub async fn get_messages(
        &self,
        topic_id: u32,
        partition_id: u32,
        offset: u64,
        count: u32,
    ) -> Result<Vec<Arc<Message>>, Error> {
        let topic = self.topics.get(&topic_id);
        if topic.is_none() {
            return Err(Error::TopicNotFound(topic_id));
        }

        topic
            .unwrap()
            .get_messages(partition_id, offset, count)
            .await
    }

    pub async fn append_messages(
        &mut self,
        topic_id: u32,
        partition_id: u32,
        messages: Vec<Message>,
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        if messages.len() > self.max_messages_in_batch {
            return Err(Error::TooManyMessages);
        }

        let topic = self.topics.get_mut(&topic_id);
        if topic.is_none() {
            return Err(Error::TopicNotFound(topic_id));
        }

        let topic = topic.unwrap();
        topic.append_messages(partition_id, messages).await?;
        Ok(())
    }
}
