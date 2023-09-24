use crate::streaming::streams::stream::Stream;

impl Stream {
    pub async fn get_messages_count(&self) -> u64 {
        let mut messages_count = 0;
        for topic in self.topics.values() {
            messages_count += topic.get_messages_count().await;
        }
        messages_count
    }
}
