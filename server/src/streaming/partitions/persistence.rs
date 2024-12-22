use crate::state::system::PartitionState;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::partitions::COMPONENT;
use error_set::ResultContext;
use iggy::error::IggyError;
use std::path::Path;
use std::sync::atomic::Ordering;
use tokio::fs::create_dir;
use tracing::error;

impl Partition {
    pub async fn load(&mut self, state: PartitionState) -> Result<(), IggyError> {
        let storage = self.storage.clone();
        storage.partition.load(self, state).await
    }

    pub async fn persist(&self) -> Result<(), IggyError> {
        self.storage.partition.save(self).await
    }

    pub async fn delete(&self) -> Result<(), IggyError> {
        for segment in &self.segments {
            self.storage
                .segment
                .delete(segment)
                .await
                .with_error(|_| format!("{COMPONENT} - failed to delete segment: {segment}",))?;
            self.segments_count_of_parent_stream
                .fetch_sub(1, Ordering::SeqCst);
        }
        self.storage.partition.delete(self).await
    }

    pub async fn purge(&mut self) -> Result<(), IggyError> {
        self.current_offset = 0;
        self.unsaved_messages_count = 0;
        self.should_increment_offset = false;
        self.consumer_offsets.clear();
        self.consumer_group_offsets.clear();
        if let Some(cache) = self.cache.as_mut() {
            cache.purge();
        }
        for segment in &self.segments {
            self.storage
                .segment
                .delete(segment)
                .await
                .with_error(|_| format!("{COMPONENT} - failed to delete segment: {segment}",))?;
            self.segments_count_of_parent_stream
                .fetch_sub(1, Ordering::SeqCst);
        }
        self.segments.clear();
        self.storage
            .partition
            .delete_consumer_offsets(&self.consumer_offsets_path)
            .await
            .with_error(|_| {
                format!("{COMPONENT} - failed to delete consumer offsets, partition: {self}")
            })?;
        self.storage
            .partition
            .delete_consumer_offsets(&self.consumer_group_offsets_path)
            .await
            .with_error(|_| {
                format!("{COMPONENT} - failed to delete consumer offsets, partition: {self}")
            })?;
        self.add_persisted_segment(0).await.with_error(|_| {
            format!("{COMPONENT} - failed to add persisted segment, partition: {self}",)
        })?;

        if !Path::new(&self.consumer_offsets_path).exists()
            && create_dir(&self.consumer_offsets_path).await.is_err()
        {
            error!(
                "Failed to create consumer offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                self.partition_id, self.stream_id, self.topic_id
            );
            return Err(IggyError::CannotCreateConsumerOffsetsDirectory(
                self.consumer_offsets_path.to_owned(),
            ));
        }

        if !Path::new(&self.consumer_group_offsets_path).exists()
            && create_dir(&self.consumer_group_offsets_path).await.is_err()
        {
            error!(
                "Failed to create consumer group offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                self.partition_id, self.stream_id, self.topic_id
            );
            return Err(IggyError::CannotCreateConsumerOffsetsDirectory(
                self.consumer_group_offsets_path.to_owned(),
            ));
        }

        Ok(())
    }
}
