use crate::streaming::io::stream::message_stream::RetainedMessageStream;
use crate::streaming::models::messages::RetainedMessage;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::segments::segment::Segment;
use crate::streaming::{batching::appendable_batch_info::AppendableBatchInfo, io::log::LogReader};
use futures::TryStreamExt;
use iggy::error::IggyError;
use iggy::messages::send_messages::Message;
use iggy::models::messages::POLLED_MESSAGE_METADATA;
use iggy::utils::sizeable::Sizeable;
use iggy::utils::timestamp::IggyTimestamp;
use std::future;
use std::sync::{atomic::Ordering, Arc};
use tracing::{error, trace, warn};

const EMPTY_MESSAGES: Vec<RetainedMessage> = vec![];

impl Partition {
    pub fn get_messages_count(&self) -> u64 {
        self.messages_count.load(Ordering::SeqCst)
    }

    pub async fn get_messages_by_timestamp(
        &self,
        timestamp: IggyTimestamp,
        count: u32,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        trace!(
            "Getting messages by timestamp: {} for partition: {}...",
            timestamp,
            self.partition_id
        );
        if self.segments.is_empty() {
            return Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect());
        }

        let timestamp = timestamp.as_micros();
        // TODO: Check cache, unsaved buffer and only if those don't contain our messages,
        // then fetch from disk.
        let mut messages = None;
        if !self.config.segment.cache_indexes {
            for segment in self.segments.iter() {
                let index = segment
                    .storage
                    .as_ref()
                    .segment
                    .try_load_index_for_timestamp(segment, timestamp)
                    .await?;

                if let Some(found) = index {
                    messages = Some(
                        segment
                            .load_n_messages_from_disk(found.position, count, |msg| {
                                msg.timestamp >= timestamp
                            })
                            .await,
                    );
                }
            }
        } else {
            for segment in self.segments.iter().rev() {
                let indexes = segment.indexes.as_ref().unwrap();
                let index = indexes
                    .iter()
                    .rposition(|time_index| time_index.timestamp <= timestamp)
                    .map(|idx| indexes[idx]);
                if index.is_none() {
                    continue;
                } else {
                    let found = index.unwrap();
                    messages = Some(
                        segment
                            .load_n_messages_from_disk(found.position, count, |msg| {
                                msg.timestamp >= timestamp
                            })
                            .await,
                    );
                }
            }
        };
        messages.unwrap_or_else(|| Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect()))
    }

    pub async fn get_messages_by_offset(
        &self,
        start_offset: u64,
        count: u32,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        trace!(
            "Getting messages for start offset: {} for partition: {}...",
            start_offset,
            self.partition_id
        );
        if self.segments.is_empty() {
            return Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect());
        }

        if start_offset > self.current_offset {
            return Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect());
        }

        let end_offset = self.get_end_offset(start_offset, count);

        let messages = self.try_get_messages_from_cache(start_offset, end_offset);
        if let Some(messages) = messages {
            return Ok(messages);
        }

        let segments = self.filter_segments_by_offsets(start_offset, end_offset);
        match segments.len() {
            0 => Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect()),
            1 => segments[0].get_messages(start_offset, count).await,
            _ => Self::get_messages_from_segments(segments, start_offset, count).await,
        }
    }

    pub async fn get_first_messages(
        &self,
        count: u32,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        self.get_messages_by_offset(0, count).await
    }

    pub async fn get_last_messages(
        &self,
        count: u32,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
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
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        let (consumer_offsets, consumer_id) = match consumer {
            PollingConsumer::Consumer(consumer_id, _) => (&self.consumer_offsets, consumer_id),
            PollingConsumer::ConsumerGroup(group_id, _) => (&self.consumer_group_offsets, group_id),
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
            return Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect());
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
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
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
    ) -> Option<Vec<Arc<RetainedMessage>>> {
        let cache = self.cache.as_ref()?;
        if cache.is_empty() || start_offset > end_offset || end_offset > self.current_offset {
            return None;
        }

        let first_buffered_offset = cache[0].offset;
        trace!(
            "First buffered offset: {} for partition: {}",
            first_buffered_offset,
            self.partition_id
        );

        if start_offset >= first_buffered_offset {
            return Some(self.load_messages_from_cache(start_offset, end_offset));
        }
        None
    }

    pub async fn get_newest_messages_by_size(
        &self,
        size_bytes: u64,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        trace!(
            "Getting messages for size: {} bytes for partition: {}...",
            size_bytes,
            self.partition_id
        );

        if self.segments.is_empty() {
            return Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect());
        }

        let mut remaining_size = size_bytes;
        let mut messages = Vec::new();
        for segment in self.segments.iter().rev() {
            let segment_size_bytes = segment.size_bytes.as_bytes_u64();
            if segment_size_bytes == 0 {
                break;
            }

            let segment_size = segment.size_bytes.as_bytes_u64();
            error!("loading from segment with size: {}", segment_size);
            if segment_size_bytes > remaining_size {
                error!("loading part of segment");
                // Last segment is bigger than the remaining size, so we need to get the newest messages from it.

                // TODO: Once the batch accumulator is refactored to flush every n blocks,
                // causing the on disk batch size to be deterministic,
                // move to calculating the start_position, instead of brute force searching from the beginning.
                let start_position = 0;
                let reader = segment
                    .log
                    .read_blocks(start_position, segment_size)
                    .into_async_read();
                let complement = segment_size - remaining_size;
                let message_stream = RetainedMessageStream::new(reader, 4096);
                let mut accumulated_size = 0;
                let collected_messages = message_stream
                    .try_filter_map(|msg| {
                        let size = msg.get_size_bytes();
                        accumulated_size += size.as_bytes_u64();
                        if accumulated_size >= complement {
                            future::ready(Ok(Some(Arc::new(msg))))
                        } else {
                            future::ready(Ok(None))
                        }
                    })
                    .try_collect::<Vec<_>>()
                    .await?;
                error!(
                    "loaded: {} messages from partial segment",
                    collected_messages.len()
                );
                messages.extend(collected_messages);
                break;
            }

            error!("loading entire segment");
            // Current segment is smaller than the remaining size, so we need to get all messages from it.
            let start_position = 0;
            let reader = segment
                .log
                .read_blocks(start_position, segment_size)
                .into_async_read();
            error!("created reader");
            let message_stream = RetainedMessageStream::new(reader, 4096);
            error!("created message stream");
            let collected_messages = message_stream
                .map_ok(Arc::new)
                .try_collect::<Vec<_>>()
                .await?;
            error!(
                "loaded {} messages from entire segment",
                collected_messages.len()
            );
            messages.extend(collected_messages);
            remaining_size = remaining_size.saturating_sub(segment_size_bytes);
            if remaining_size == 0 {
                break;
            }
        }
        Ok(messages)
    }

    fn load_messages_from_cache(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Vec<Arc<RetainedMessage>> {
        trace!(
            "Loading messages from cache, start offset: {}, end offset: {}...",
            start_offset,
            end_offset
        );

        if self.cache.is_none() || start_offset > end_offset {
            return EMPTY_MESSAGES.into_iter().map(Arc::new).collect();
        }

        let cache = self.cache.as_ref().unwrap();
        if cache.is_empty() {
            return EMPTY_MESSAGES.into_iter().map(Arc::new).collect();
        }

        let first_offset = cache[0].offset;
        let start_index = (start_offset - first_offset) as usize;
        let end_index = usize::min(cache.len(), (end_offset - first_offset + 1) as usize);
        let expected_messages_count = end_index - start_index;

        let mut messages = Vec::with_capacity(expected_messages_count);
        for i in start_index..end_index {
            messages.push(cache[i].clone());
        }

        if messages.len() != expected_messages_count {
            warn!(
                "Loaded {} messages from cache, expected {}.",
                messages.len(),
                expected_messages_count
            );
            return EMPTY_MESSAGES.into_iter().map(Arc::new).collect();
        }

        trace!(
            "Loaded {} messages from cache, start offset: {}, end offset: {}...",
            messages.len(),
            start_offset,
            end_offset
        );

        messages
    }

    pub async fn append_messages(
        &mut self,
        appendable_batch_info: AppendableBatchInfo,
        messages: Vec<Message>,
    ) -> Result<(), IggyError> {
        {
            let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
            if last_segment.is_closed {
                let start_offset = last_segment.end_offset + 1;
                let unsaved_messages = last_segment.unsaved_messages.clone();
                trace!(
                    "Current segment is closed, creating new segment with start offset: {} for partition with ID: {}...",
                    start_offset, self.partition_id
                );
                // Rolling over messages that couldn't be persisted during segment close.
                self.add_persisted_segment(unsaved_messages, start_offset)
                    .await?;
            }
        }

        let batch_size = appendable_batch_info.batch_size
            + ((POLLED_MESSAGE_METADATA * messages.len() as u32) as u64).into();
        let base_offset = if !self.should_increment_offset {
            0
        } else {
            self.current_offset + 1
        };

        let mut messages_count = 0u32;
        let mut retained_messages = Vec::with_capacity(messages.len());
        if let Some(message_deduplicator) = &self.message_deduplicator {
            for message in messages {
                if !message_deduplicator.try_insert(&message.id).await {
                    warn!(
                        "Ignored the duplicated message ID: {} for partition with ID: {}.",
                        message.id, self.partition_id
                    );
                    continue;
                }
                let timestamp = IggyTimestamp::now().as_micros();
                let message_offset = base_offset + messages_count as u64;
                let message = Arc::new(RetainedMessage::new(message_offset, timestamp, message));
                retained_messages.push(message.clone());
                messages_count += 1;
            }
        } else {
            for message in messages {
                let timestamp = IggyTimestamp::now().as_micros();
                let message_offset = base_offset + messages_count as u64;
                let message = Arc::new(RetainedMessage::new(message_offset, timestamp, message));
                retained_messages.push(message.clone());
                messages_count += 1;
            }
        }
        if messages_count == 0 {
            return Ok(());
        }

        let last_offset = base_offset + (messages_count - 1) as u64;
        if self.should_increment_offset {
            self.current_offset = last_offset;
        } else {
            self.should_increment_offset = true;
            self.current_offset = last_offset;
        }

        {
            let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
            last_segment
                .append_batch(batch_size, messages_count, &retained_messages)
                .await?;
        }

        if let Some(cache) = &mut self.cache {
            cache.extend(retained_messages);
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

                last_segment.persist_messages(false).await?;
                self.unsaved_messages_count = 0;
            }
        }

        Ok(())
    }

    pub async fn flush_unsaved_buffer(&mut self, fsync: bool) -> Result<(), IggyError> {
        let _fsync = fsync;
        if self.unsaved_messages_count == 0 {
            return Ok(());
        }

        let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
        trace!(
            "Segment with start offset: {} for partition with ID: {} will be forcefully persisted on disk...",
            last_segment.start_offset,
            self.partition_id
        );

        // Make sure all of the messages from the accumulator are persisted
        // no leftover from one round trip.
        while last_segment.unsaved_messages.is_some() {
            last_segment.persist_messages(false).await?;
        }
        self.unsaved_messages_count = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use iggy::utils::byte_size::IggyByteSize;
    use iggy::utils::expiry::IggyExpiry;
    use iggy::utils::sizeable::Sizeable;
    use std::sync::atomic::{AtomicU32, AtomicU64};

    use super::*;
    use crate::configs::system::{MessageDeduplicationConfig, SystemConfig};
    use crate::streaming::iggy_storage::tests::get_test_system_storage;
    use crate::streaming::partitions::create_messages;

    #[tokio::test]
    async fn given_disabled_message_deduplication_all_messages_should_be_appended() {
        let mut partition = create_partition(false);
        let messages = create_messages();
        let messages_count = messages.len() as u32;
        let appendable_batch_info = AppendableBatchInfo {
            batch_size: messages
                .iter()
                .map(|m| m.get_size_bytes())
                .sum::<IggyByteSize>(),
            partition_id: partition.partition_id,
        };
        partition
            .append_messages(appendable_batch_info, messages)
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
        let appendable_batch_info = AppendableBatchInfo {
            batch_size: messages
                .iter()
                .map(|m| m.get_size_bytes())
                .sum::<IggyByteSize>(),
            partition_id: partition.partition_id,
        };
        partition
            .append_messages(appendable_batch_info, messages)
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
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyTimestamp::now(),
        )
    }
}
