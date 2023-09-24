use crate::streaming::streams::stream::Stream;

impl Stream {
    pub fn get_partitions_count(&self) -> u32 {
        let mut partitions_count = 0;
        for topic in self.topics.values() {
            partitions_count += topic.get_partitions_count();
        }

        partitions_count
    }
}
