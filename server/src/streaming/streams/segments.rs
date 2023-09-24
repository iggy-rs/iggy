use crate::streaming::streams::stream::Stream;

impl Stream {
    pub async fn get_segments_count(&self) -> u32 {
        let mut segments_count = 0;
        for topic in self.topics.values() {
            segments_count += topic.get_segments_count().await;
        }

        segments_count
    }
}
