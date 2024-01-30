use crate::streaming::partitions::partition::Partition;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::utils::random_id;
use iggy::batching::batcher::Batcher;
use iggy::batching::batches_converter::BatchesConverter;
use iggy::batching::messages_batch::{MessageBatch, MessageBatchAttributes};
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::error::IggyError;
use iggy::models::messages::Message;
use std::sync::Arc;
use tracing::{trace, warn};

const EMPTY_MESSAGES: Vec<Message> = vec![];
const EMPTY_BATCHES: Vec<MessageBatch> = vec![];

impl Partition {
    pub fn get_messages_count(&self) -> u64 {
        let first_segment_offset = self.segments.first().map_or_else(
            || 0,
            |segment| {
                if segment.current_size_bytes == 0 {
                    return 0;
                }
                segment.start_offset
            },
        );
        let last_segment_offset = self
            .segments
            .last()
            .map_or_else(|| 0, |segment| segment.current_offset);

        if last_segment_offset > 0 {
            last_segment_offset - first_segment_offset + 1
        } else {
            last_segment_offset - first_segment_offset
        }
    }

    pub async fn get_messages_by_timestamp(
        &self,
        timestamp: u64,
        count: u32,
    ) -> Result<Vec<Message>, IggyError> {
        trace!(
            "Getting messages by timestamp: {} for partition: {}...",
            timestamp,
            self.partition_id
        );
        if self.segments.is_empty() {
            return Ok(EMPTY_MESSAGES);
        }

        let result = self.segments.iter().rev().find_map(|segment| {
            segment.time_indexes.as_ref().and_then(|time_indexes| {
                if time_indexes.is_empty() {
                    return None;
                }

                time_indexes
                    .iter()
                    .rposition(|seg| seg.timestamp <= timestamp)
                    .map(|idx| {
                        let offset =
                            segment.start_offset + time_indexes[idx].relative_offset as u64;
                        self.get_messages_by_offset(offset, count)
                    })
                    .or_else(|| {
                        let first_time_index = TimeIndex::default();
                        let offset = first_time_index.relative_offset as u64;
                        Some(self.get_messages_by_offset(offset, count))
                    })
            })
        });

        match result {
            Some(res) => res.await,
            None => Ok(EMPTY_MESSAGES),
        }
    }

    pub async fn get_messages_by_offset(
        &self,
        start_offset: u64,
        count: u32,
    ) -> Result<Vec<Message>, IggyError> {
        trace!(
            "Getting messages for start offset: {} for partition: {}...",
            start_offset,
            self.partition_id
        );
        if self.segments.is_empty() {
            return Ok(EMPTY_MESSAGES);
        }

        if start_offset > self.current_offset {
            return Ok(EMPTY_MESSAGES);
        }

        let end_offset = self.get_end_offset(start_offset, count);

        let messages = self.try_get_messages_from_cache(start_offset, end_offset);
        if let Some(messages) = messages {
            return Ok(messages);
        }

        let segments = self.filter_segments_by_offsets(start_offset, end_offset);
        match segments.len() {
            0 => Ok(EMPTY_MESSAGES),
            1 => segments[0].get_messages(start_offset, count).await,
            _ => Self::get_messages_from_segments(segments, start_offset, count).await,
        }
    }

    pub async fn get_first_messages(&self, count: u32) -> Result<Vec<Message>, IggyError> {
        self.get_messages_by_offset(0, count).await
    }

    pub async fn get_last_messages(&self, count: u32) -> Result<Vec<Message>, IggyError> {
        let mut count = count as u64;
        if count > self.current_offset + 1 {
            count = self.current_offset + 1
        }

        let start_offset = 1 + self.current_offset - count;
        self.get_messages_by_offset(start_offset, count as u32)
            .await
    }

    pub async fn get_next_messages(
        &self,
        consumer: PollingConsumer,
        count: u32,
    ) -> Result<Vec<Message>, IggyError> {
        let (consumer_offsets, consumer_id) = match consumer {
            PollingConsumer::Consumer(consumer_id, _) => (&self.consumer_offsets, consumer_id),
            PollingConsumer::ConsumerGroup(consumer_group_id, _) => {
                (&self.consumer_group_offsets, consumer_group_id)
            }
        };

        let consumer_offset = consumer_offsets.get(&consumer_id);
        if consumer_offset.is_none() {
            trace!(
                "Consumer: {} hasn't stored offset for partition: {}, returning the first messages...",
                consumer_id,
                self.partition_id
            );
            return self.get_first_messages(count).await;
        }

        let consumer_offset = consumer_offset.unwrap();
        if consumer_offset.offset == self.current_offset {
            trace!(
                "Consumer: {} has the latest offset: {} for partition: {}, returning empty messages...",
                consumer_id,
                consumer_offset.offset,
                self.partition_id
            );
            return Ok(EMPTY_MESSAGES);
        }

        let offset = consumer_offset.offset + 1;
        trace!(
            "Getting next messages for {} for partition: {} from offset: {}...",
            consumer_id,
            self.partition_id,
            offset
        );

        self.get_messages_by_offset(offset, count).await
    }

    fn get_end_offset(&self, offset: u64, count: u32) -> u64 {
        let mut end_offset = offset + (count - 1) as u64;
        let segment = self.segments.last().unwrap();
        let max_offset = segment.current_offset;
        if end_offset > max_offset {
            end_offset = max_offset;
        }

        end_offset
    }

    fn filter_segments_by_offsets(&self, start_offset: u64, end_offset: u64) -> Vec<&Segment> {
        let slice_start = self
            .segments
            .iter()
            .rposition(|segment| segment.start_offset <= start_offset)
            .unwrap_or(0);

        self.segments[slice_start..]
            .iter()
            .filter(|segment| segment.start_offset <= end_offset)
            .collect()
    }
    async fn get_messages_from_segments(
        segments: Vec<&Segment>,
        offset: u64,
        count: u32,
    ) -> Result<Vec<Message>, IggyError> {
        let mut messages = Vec::with_capacity(segments.len());
        for segment in segments {
            let segment_messages = segment.get_messages(offset, count).await?;
            for message in segment_messages {
                messages.push(message);
            }
        }

        Ok(messages)
    }

    fn try_get_messages_from_cache(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Option<Vec<Message>> {
        let cache = self.cache.as_ref()?;
        if cache.is_empty() || start_offset > end_offset || end_offset > self.current_offset {
            return None;
        }

        let first_buffered_offset = cache[0].base_offset;
        trace!(
            "First buffered offset: {} for partition: {}",
            first_buffered_offset,
            self.partition_id
        );

        if start_offset >= first_buffered_offset {
            return self.load_messages_from_cache(start_offset, end_offset).ok();
        }
        None
    }

    pub async fn get_newest_messages_by_size(
        &self,
        size_bytes: u32,
    ) -> Result<Vec<Arc<MessageBatch>>, IggyError> {
        trace!(
            "Getting messages for size: {} bytes for partition: {}...",
            size_bytes,
            self.partition_id
        );

        if self.segments.is_empty() {
            return Ok(EMPTY_BATCHES.into_iter().map(Arc::new).collect::<Vec<_>>());
        }

        let mut remaining_size = size_bytes;
        let mut batches = Vec::new();
        for segment in self.segments.iter().rev() {
            let segment_size_bytes = segment.current_size_bytes;
            if segment_size_bytes == 0 {
                break;
            }
            if segment_size_bytes > remaining_size {
                // Last segment is bigger than the remaining size, so we need to get the newest messages from it.
                let partial_messages = segment
                    .get_newest_message_batches_by_size(remaining_size)
                    .await?;
                batches.splice(..0, partial_messages);
                break;
            }

            // Current segment is smaller than the remaining size, so we need to get all messages from it.
            let segment_batches = segment.get_all_batches().await?;
            batches.splice(..0, segment_batches);
            remaining_size = remaining_size.saturating_sub(segment_size_bytes);
            if remaining_size == 0 {
                break;
            }
        }

        Ok(batches)
    }

    fn load_messages_from_cache(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<Message>, IggyError> {
        trace!(
            "Loading messages from cache, start offset: {}, end offset: {}...",
            start_offset,
            end_offset
        );

        if self.cache.is_none() || start_offset > end_offset {
            return Ok(EMPTY_MESSAGES);
        }

        let cache = self.cache.as_ref().unwrap();
        if cache.is_empty() {
            return Ok(EMPTY_MESSAGES);
        }

        let mut slice_start = 0;
        for idx in (0..cache.len()).rev() {
            if cache[idx].base_offset <= start_offset {
                slice_start = idx;
                break;
            }
        }
        let messages = cache
            .iter()
            .skip(slice_start)
            .filter_map(|batch| {
                if batch.is_contained_or_overlapping_within_offset_range(start_offset, end_offset) {
                    Some(batch.clone())
                } else {
                    None
                }
            })
            .convert_and_filter_by_offset_range(start_offset, end_offset)?;

        let expected_messages_count = (end_offset - start_offset + 1) as usize;
        if messages.len() != expected_messages_count {
            warn!(
                "Loaded {} messages from cache, expected {}.",
                messages.len(),
                expected_messages_count
            );
            return Ok(EMPTY_MESSAGES);
        }
        trace!(
            "Loaded {} messages from cache, start offset: {}, end offset: {}...",
            messages.len(),
            start_offset,
            end_offset
        );

        Ok(messages)
    }

    pub async fn append_messages(
        &mut self,
        compression_algorithm: CompressionAlgorithm,
        messages: Vec<Message>,
    ) -> Result<(), IggyError> {
        {
            let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
            if last_segment.is_closed {
                let start_offset = last_segment.end_offset + 1;
                trace!(
                    "Current segment is closed, creating default segment with start offset: {} for partition with ID: {}...",
                    start_offset, self.partition_id
                );
                self.add_persisted_segment(start_offset).await?;
            }
        }

        let begin_offset = if !self.should_increment_offset {
            0
        } else {
            self.current_offset + 1
        };

        // TODO(numinex) - lets keep it like that for now, in the future when producer side compression
        // is implemented will change this to offset_delta (u32 instead of u64)
        let mut curr_offset = begin_offset;
        // assume that messages have monotonic timestamps
        let mut max_timestamp = 0;

        let mut appendable_messages = Vec::with_capacity(messages.len());
        if let Some(message_deduplicator) = &mut self.message_deduplicator {
            for mut message in messages {
                if message.id == 0 {
                    message.id = random_id::get_uuid();
                }

                if !message_deduplicator.try_insert(&message.id).await {
                    warn!(
                        "Ignored the duplicated message ID: {} for partition with ID: {}.",
                        message.id, self.partition_id
                    );
                    continue;
                }

                message.offset = curr_offset;
                max_timestamp = message.timestamp;
                curr_offset += 1;

                appendable_messages.push(message);
            }
        } else {
            for mut message in messages {
                if message.id == 0 {
                    message.id = random_id::get_uuid();
                }
                message.offset = curr_offset;
                max_timestamp = message.timestamp;
                curr_offset += 1;

                appendable_messages.push(message);
            }
        }
        let messages_count = appendable_messages.len() as u32;

        let last_offset = curr_offset - 1;
        if self.should_increment_offset {
            self.current_offset = last_offset;
        } else {
            self.should_increment_offset = true;
            self.current_offset = last_offset;
        }
        let last_offset_delta = (last_offset - begin_offset) as u32;

        let attributes = MessageBatchAttributes::new(compression_algorithm).into();
        let batch = appendable_messages.into_batch(begin_offset, last_offset_delta, attributes)?;
        {
            let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
            last_segment
                .append_message_batches(batch.clone(), last_offset, max_timestamp)
                .await?;
        }
        if let Some(cache) = &mut self.cache {
            //cache.extend(vec![batch.clone()]);
            cache.push_safe(batch.clone());
        }

        self.unsaved_messages_count += messages_count;
        {
            let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
            if self.unsaved_messages_count >= self.config.partition.messages_required_to_save
                || last_segment.is_full().await
            {
                trace!(
                    "Segment with start offset: {} for partition with ID: {} will be persisted on disk...",
                    last_segment.start_offset,
                    self.partition_id
                );
                last_segment.persist_messages().await?;
                self.unsaved_messages_count = 0;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::{MessageDeduplicationConfig, SystemConfig};
    use crate::streaming::partitions::create_messages;
    use crate::streaming::storage::tests::get_test_system_storage;

    #[tokio::test]
    async fn given_disabled_message_deduplication_all_messages_should_be_appended() {
        let mut partition = create_partition(false);
        let messages = create_messages();
        let messages_count = messages.len() as u32;
        partition
            .append_messages(CompressionAlgorithm::None, messages)
            .await
            .unwrap();

        let loaded_messages = partition
            .get_messages_by_offset(0, messages_count)
            .await
            .unwrap();
        assert_eq!(loaded_messages.len(), messages_count as usize);
    }

    #[tokio::test]
    async fn given_enabled_message_deduplication_only_messages_with_unique_id_should_be_appended() {
        let mut partition = create_partition(true);
        let messages = create_messages();
        let messages_count = messages.len() as u32;
        let unique_messages_count = 3;
        partition
            .append_messages(CompressionAlgorithm::None, messages)
            .await
            .unwrap();

        let loaded_messages = partition
            .get_messages_by_offset(0, messages_count)
            .await
            .unwrap();
        assert_eq!(loaded_messages.len(), unique_messages_count);
    }

    fn create_partition(deduplication_enabled: bool) -> Partition {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let with_segment = true;
        let config = Arc::new(SystemConfig {
            message_deduplication: MessageDeduplicationConfig {
                enabled: deduplication_enabled,
                ..Default::default()
            },
            ..Default::default()
        });
        Partition::create(
            stream_id,
            topic_id,
            partition_id,
            with_segment,
            config,
            storage,
            None,
        )
    }
}
