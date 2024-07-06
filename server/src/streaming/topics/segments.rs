use crate::streaming::topics::topic::Topic;

impl Topic {
    pub async fn get_segments_count(&self) -> u32 {
        let mut segments_count = 0;
        for partition in self.partitions.borrow().values() {
            segments_count += partition.get_segments_count();
        }

        segments_count
    }
}
