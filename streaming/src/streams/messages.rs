use crate::message::Message;
use crate::storage::SegmentStorage;
use crate::streams::stream::Stream;
use sdk::error::Error;
use sdk::messages::poll_messages::Kind;
use sdk::messages::send_messages::KeyKind;
use std::sync::Arc;

impl Stream {
    pub async fn get_messages(
        &self,
        consumer_id: u32,
        topic_id: u32,
        partition_id: u32,
        kind: Kind,
        value: u64,
        count: u32,
    ) -> Result<Vec<Arc<Message>>, Error> {
        let topic = self.topics.get(&topic_id);
        if topic.is_none() {
            return Err(Error::TopicNotFound(topic_id, self.id));
        }

        topic
            .unwrap()
            .get_messages(consumer_id, partition_id, kind, value, count)
            .await
    }

    pub async fn append_messages(
        &self,
        topic_id: u32,
        key_kind: KeyKind,
        key_value: u32,
        messages: Vec<Message>,
        storage: Arc<dyn SegmentStorage>,
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        let topic = self.topics.get(&topic_id);
        if topic.is_none() {
            return Err(Error::TopicNotFound(topic_id, self.id));
        }

        let topic = topic.unwrap();
        topic
            .append_messages(key_kind, key_value, messages, storage)
            .await
    }
}
