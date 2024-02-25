use crate::streaming::topics::topic::Topic;
use iggy::locking::IggySharedMutFn;

impl Topic {
    pub async fn get_segments_count(&self) -> u32 {
        let mut segments_count = 0;
        for partition in self.partitions.values() {
            segments_count += partition.read().await.get_segments_count();
        }

        segments_count
    }
}
