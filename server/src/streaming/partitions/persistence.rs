use crate::streaming::partitions::partition::Partition;
use iggy::consumer::ConsumerKind;
use iggy::error::IggyError;

impl Partition {
    pub async fn load(&mut self) -> Result<(), IggyError> {
        let storage = self.storage.clone();
        storage.partition.load(self).await
    }

    pub async fn persist(&self) -> Result<(), IggyError> {
        self.storage.partition.save(self).await
    }

    pub async fn delete(&self) -> Result<(), IggyError> {
        self.storage.partition.delete(self).await
    }

    pub async fn purge(&mut self) -> Result<(), IggyError> {
        self.current_offset = 0;
        self.unsaved_messages_count = 0;
        self.should_increment_offset = false;
        if let Some(cache) = self.cache.as_mut() {
            cache.purge();
        }
        for segment in &self.segments {
            self.storage.segment.delete(segment).await?;
        }
        self.segments.clear();
        self.storage
            .partition
            .delete_consumer_offsets(
                ConsumerKind::Consumer,
                self.stream_id,
                self.topic_id,
                self.partition_id,
            )
            .await?;
        self.storage
            .partition
            .delete_consumer_offsets(
                ConsumerKind::ConsumerGroup,
                self.stream_id,
                self.topic_id,
                self.partition_id,
            )
            .await?;
        self.add_persisted_segment(0).await?;
        Ok(())
    }
}
