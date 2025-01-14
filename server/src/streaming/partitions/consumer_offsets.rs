use crate::streaming::partitions::partition::{ConsumerOffset, Partition};
use crate::streaming::partitions::COMPONENT;
use crate::streaming::polling_consumer::PollingConsumer;
use dashmap::DashMap;
use error_set::ErrContext;
use iggy::consumer::ConsumerKind;
use iggy::error::IggyError;
use tracing::trace;

impl Partition {
    pub async fn get_consumer_offset(
        &self,
        consumer: PollingConsumer,
    ) -> Result<Option<u64>, IggyError> {
        trace!(
            "Getting consumer offset for {}, partition: {}, current: {}...",
            consumer,
            self.partition_id,
            self.current_offset
        );

        match consumer {
            PollingConsumer::Consumer(consumer_id, _) => {
                let consumer_offset = self.consumer_offsets.get(&consumer_id);
                if let Some(consumer_offset) = consumer_offset {
                    return Ok(Some(consumer_offset.offset));
                }
            }
            PollingConsumer::ConsumerGroup(consumer_group_id, _) => {
                let consumer_offset = self.consumer_offsets.get(&consumer_group_id);
                if let Some(consumer_offset) = consumer_offset {
                    return Ok(Some(consumer_offset.offset));
                }
            }
        }

        Ok(None)
    }

    pub async fn store_consumer_offset(
        &self,
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<(), IggyError> {
        trace!(
            "Storing offset: {} for {}, partition: {}, current: {}...",
            offset,
            consumer,
            self.partition_id,
            self.current_offset
        );
        if offset > self.current_offset {
            return Err(IggyError::InvalidOffset(offset));
        }

        match consumer {
            PollingConsumer::Consumer(consumer_id, _) => {
                self.store_offset(ConsumerKind::Consumer, consumer_id, offset)
                    .await
                    .with_error_context(|_| format!("{COMPONENT} - failed to store consumer offset, consumer ID: {}, offset: {}", consumer_id, offset))?;
            }
            PollingConsumer::ConsumerGroup(consumer_id, _) => {
                self.store_offset(ConsumerKind::ConsumerGroup, consumer_id, offset)
                    .await
                    .with_error_context(|_| format!("{COMPONENT} - failed to store consumer group offset, consumer ID: {}, offset: {}", consumer_id, offset))?;
            }
        };

        Ok(())
    }

    async fn store_offset(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
        offset: u64,
    ) -> Result<(), IggyError> {
        let consumer_offsets = self.get_consumer_offsets(kind);
        if let Some(mut consumer_offset) = consumer_offsets.get_mut(&consumer_id) {
            consumer_offset.offset = offset;
            self.storage
                .partition
                .save_consumer_offset(&consumer_offset)
                .await
                .with_error_context(|_| {
                    format!(
                        "{COMPONENT} - failed to save consumer offset, consumer ID: {}, offset: {}",
                        consumer_id, offset
                    )
                })?;
            return Ok(());
        }

        let path = match kind {
            ConsumerKind::Consumer => &self.consumer_offsets_path,
            ConsumerKind::ConsumerGroup => &self.consumer_group_offsets_path,
        };
        let consumer_offset = ConsumerOffset::new(kind, consumer_id, offset, path);
        self.storage
            .partition
            .save_consumer_offset(&consumer_offset)
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to save new consumer offset, consumer ID: {}, offset: {}",
                    consumer_id, offset
                )
            })?;
        consumer_offsets.insert(consumer_id, consumer_offset);
        Ok(())
    }

    pub async fn load_consumer_offsets(&mut self) -> Result<(), IggyError> {
        trace!(
            "Loading consumer offsets for partition with ID: {} for topic with ID: {} and stream with ID: {}...",
            self.partition_id,
            self.topic_id,
            self.stream_id
        );
        self.load_consumer_offsets_from_storage(ConsumerKind::Consumer)
            .await
            .with_error_context(|_| {
                format!("{COMPONENT} - failed to load consumer offsets from storage")
            })?;
        self.load_consumer_offsets_from_storage(ConsumerKind::ConsumerGroup)
            .await
    }

    async fn load_consumer_offsets_from_storage(
        &self,
        kind: ConsumerKind,
    ) -> Result<(), IggyError> {
        let path = match kind {
            ConsumerKind::Consumer => &self.consumer_offsets_path,
            ConsumerKind::ConsumerGroup => &self.consumer_group_offsets_path,
        };
        let loaded_consumer_offsets = self
            .storage
            .partition
            .load_consumer_offsets(kind, path)
            .await
            .with_error_context(|_| {
                format!("{COMPONENT} - failed to load consumer offsets, kind: {kind}, path: {path}")
            })?;
        let consumer_offsets = self.get_consumer_offsets(kind);
        for consumer_offset in loaded_consumer_offsets {
            self.log_consumer_offset(&consumer_offset);
            consumer_offsets.insert(consumer_offset.consumer_id, consumer_offset);
        }
        Ok(())
    }

    fn get_consumer_offsets(&self, kind: ConsumerKind) -> &DashMap<u32, ConsumerOffset> {
        match kind {
            ConsumerKind::Consumer => &self.consumer_offsets,
            ConsumerKind::ConsumerGroup => &self.consumer_group_offsets,
        }
    }

    fn log_consumer_offset(&self, consumer_offset: &ConsumerOffset) {
        trace!("Loaded consumer offset value: {} for {} with ID: {} for partition with ID: {} for topic with ID: {} and stream with ID: {}.",
                consumer_offset.offset,
                consumer_offset.kind,
                consumer_offset.consumer_id,
                self.partition_id,
                self.topic_id,
                self.stream_id
            );
    }

    pub async fn delete_consumer_offset(
        &mut self,
        consumer: PollingConsumer,
    ) -> Result<(), IggyError> {
        let partition_id = self.partition_id;
        trace!(
            "Deleting consumer offset for consumer: {consumer}, partition ID: {partition_id}..."
        );
        match consumer {
            PollingConsumer::Consumer(consumer_id, _) => {
                let (_, offset) = self
                    .consumer_offsets
                    .remove(&consumer_id)
                    .ok_or(IggyError::ConsumerOffsetNotFound(consumer_id))?;
                self.storage.partition.delete_consumer_offset(&offset.path).await
                    .with_error_context(|_| format!("{COMPONENT} - failed to delete consumer offset, consumer ID: {consumer_id}, partition ID: {partition_id}"))?;
            }
            PollingConsumer::ConsumerGroup(consumer_id, _) => {
                let (_, offset) = self
                    .consumer_group_offsets
                    .remove(&consumer_id)
                    .ok_or(IggyError::ConsumerOffsetNotFound(consumer_id))?;
                self.storage.partition.delete_consumer_offset(&offset.path).await
                    .with_error_context(|_| format!("{COMPONENT} - failed to delete consumer group offset, consumer ID: {consumer_id}, partition ID: {partition_id}"))?;
            }
        };
        trace!("Deleted consumer offset for consumer: {consumer}, partition ID: {partition_id}.");
        Ok(())
    }
}
