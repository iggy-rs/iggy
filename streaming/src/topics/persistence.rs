use crate::storage::SegmentStorage;
use crate::topics::topic::Topic;
use iggy::error::Error;
use std::sync::Arc;

impl Topic {
    pub async fn load(&mut self) -> Result<(), Error> {
        let storage = self.storage.clone();
        storage.topic.load(self).await?;
        storage.topic.load_consumer_groups(self).await
    }

    pub async fn persist(&self) -> Result<(), Error> {
        self.storage.topic.save(self).await
    }

    pub async fn delete(&self) -> Result<(), Error> {
        self.storage.topic.delete(self).await
    }

    pub async fn persist_messages(&self, storage: Arc<dyn SegmentStorage>) -> Result<(), Error> {
        for partition in self.get_partitions() {
            let mut partition = partition.write().await;
            for segment in partition.get_segments_mut() {
                segment.persist_messages(storage.clone()).await?;
            }
        }

        Ok(())
    }
}
