use crate::streaming::partitions::partition::Partition;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::segments::segment::Segment;
use crate::streaming::utils::random_id;
use iggy::error::Error;
use iggy::models::messages::Message;
use std::sync::Arc;
use tracing::{trace, warn};

const EMPTY_MESSAGES: Vec<Arc<Message>> = vec![];

impl Partition {
    pub fn get_messages_count(&self) -> u64 {
        let first_segment = self.segments.first();
        if first_segment.is_none() {
            return 0;
        }

        let first_segment = first_segment.unwrap();
        if first_segment.current_size_bytes == 0 {
            return 0;
        }

        let last_segment = self.segments.last().unwrap();
        last_segment.current_offset - first_segment.start_offset + 1
    }

    pub async fn get_messages_by_timestamp(
        &self,
        timestamp: u64,
        count: u32,
    ) -> Result<Vec<Arc<Message>>, Error> {
        trace!(
            "Getting messages by timestamp: {} for partition: {}...",
            timestamp,
            self.partition_id
        );
        if self.segments.is_empty() {
            return Ok(EMPTY_MESSAGES);
        }

        let mut maybe_start_offset = None;
        for segment in self.segments.iter() {
            if segment.time_indexes.is_none() {
                continue;
            }

            let time_indexes = segment.time_indexes.as_ref().unwrap();
            if time_indexes.is_empty() {
                continue;
            }

            let first_timestamp = time_indexes.first().unwrap().timestamp;
            let last_timestamp = time_indexes.last().unwrap().timestamp;
            if timestamp < first_timestamp || timestamp > last_timestamp {
                continue;
            }

            let relative_start_offset = time_indexes
                .iter()
                .find(|time_index| time_index.timestamp >= timestamp)
                .map(|time_index| time_index.relative_offset)
                .unwrap_or(0);

            let start_offset = segment.start_offset + relative_start_offset as u64;
            maybe_start_offset = Some(start_offset);
            trace!(
                "Found start offset: {} for timestamp: {}.",
                start_offset,
                timestamp
            );

            break;
        }

        if maybe_start_offset.is_none() {
            trace!("Start offset for timestamp: {} was not found.", timestamp);
            return Ok(EMPTY_MESSAGES);
        }

        self.get_messages_by_offset(maybe_start_offset.unwrap(), count)
            .await
    }

    pub async fn get_messages_by_offset(
        &self,
        start_offset: u64,
        count: u32,
    ) -> Result<Vec<Arc<Message>>, Error> {
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

    pub async fn get_first_messages(&self, count: u32) -> Result<Vec<Arc<Message>>, Error> {
        self.get_messages_by_offset(0, count).await
    }

    pub async fn get_last_messages(&self, count: u32) -> Result<Vec<Arc<Message>>, Error> {
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
    ) -> Result<Vec<Arc<Message>>, Error> {
        let (consumer_offsets, consumer_id) = match consumer {
            PollingConsumer::Consumer(consumer_id, _) => {
                (self.consumer_offsets.read().await, consumer_id)
            }
            PollingConsumer::ConsumerGroup(consumer_group_id, _) => {
                (self.consumer_group_offsets.read().await, consumer_group_id)
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

    fn filter_segments_by_offsets(&self, offset: u64, end_offset: u64) -> Vec<&Segment> {
        self.segments
            .iter()
            .filter(|segment| {
                (segment.start_offset >= offset && segment.current_offset <= end_offset)
                    || (segment.start_offset <= offset && segment.current_offset >= offset)
                    || (segment.start_offset <= end_offset && segment.current_offset >= end_offset)
            })
            .collect::<Vec<&Segment>>()
    }

    async fn get_messages_from_segments(
        segments: Vec<&Segment>,
        offset: u64,
        count: u32,
    ) -> Result<Vec<Arc<Message>>, Error> {
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
    ) -> Option<Vec<Arc<Message>>> {
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
        size_bytes: u32,
    ) -> Result<Vec<Arc<Message>>, Error> {
        trace!(
            "Getting messages for size: {} bytes for partition: {}...",
            size_bytes,
            self.partition_id
        );

        if self.segments.is_empty() {
            return Ok(EMPTY_MESSAGES);
        }

        let mut remaining_size = size_bytes as u64;
        let mut messages = Vec::new();
        for segment in self.segments.iter().rev() {
            let segment_size_bytes = segment.current_size_bytes as u64;
            if segment_size_bytes > remaining_size {
                // Last segment is bigger than the remaining size, so we need to get the newest messages from it.
                let partial_messages = segment.get_newest_messages_by_size(remaining_size).await?;
                messages.splice(..0, partial_messages);
                break;
            }

            // Current segment is smaller than the remaining size, so we need to get all messages from it.
            let segment_messages = segment.get_all_messages().await?;
            messages.splice(..0, segment_messages);
            remaining_size = remaining_size.saturating_sub(segment_size_bytes);
            if remaining_size == 0 {
                break;
            }
        }

        Ok(messages)
    }

    fn load_messages_from_cache(&self, start_offset: u64, end_offset: u64) -> Vec<Arc<Message>> {
        trace!(
            "Loading messages from cache, start offset: {}, end offset: {}...",
            start_offset,
            end_offset
        );

        if self.cache.is_none() || start_offset > end_offset {
            return EMPTY_MESSAGES;
        }

        let cache = self.cache.as_ref().unwrap();
        if cache.is_empty() {
            return EMPTY_MESSAGES;
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
            return EMPTY_MESSAGES;
        }

        trace!(
            "Loaded {} messages from cache, start offset: {}, end offset: {}...",
            messages.len(),
            start_offset,
            end_offset
        );

        messages
    }

    pub async fn append_messages(&mut self, mut messages: Vec<Message>) -> Result<(), Error> {
        {
            let last_segment = self.segments.last_mut().ok_or(Error::SegmentNotFound)?;

            if last_segment.is_closed {
                let start_offset = last_segment.end_offset + 1;
                trace!(
                    "Current segment is closed, creating new segment with start offset: {} for partition with ID: {}...",
                    start_offset, self.partition_id
                );
                self.add_persisted_segment(start_offset).await?;
            }
        }

        for message in &mut messages {
            if message.id == 0 {
                message.id = random_id::get_uuid();
            }
        }

        if let Some(message_deduplicator) = &mut self.message_deduplicator {
            let mut deduplicated_messages = Vec::with_capacity(messages.len());
            for message in messages {
                if message_deduplicator.try_insert(&message.id).await {
                    deduplicated_messages.push(message);
                } else {
                    warn!(
                        "Ignored the duplicated message ID: {} for partition with ID: {}.",
                        message.id, self.partition_id
                    );
                }
            }

            messages = deduplicated_messages;
        }

        let messages_count = messages.len() as u32;
        for message in &mut messages {
            if self.should_increment_offset {
                self.current_offset += 1;
            } else {
                self.should_increment_offset = true;
            }
            message.offset = self.current_offset;
        }

        let messages = messages.into_iter().map(Arc::new).collect::<Vec<_>>();
        {
            let last_segment = self.segments.last_mut().ok_or(Error::SegmentNotFound)?;
            last_segment.append_messages(&messages).await?;
        }

        if let Some(cache) = &mut self.cache {
            cache.extend(messages);
        }

        self.unsaved_messages_count += messages_count;
        {
            let last_segment = self.segments.last_mut().ok_or(Error::SegmentNotFound)?;
            if self.unsaved_messages_count >= self.config.partition.messages_required_to_save
                || last_segment.is_full().await
            {
                trace!(
                    "Segment with start offset: {} for partition with ID: {} will be persisted on disk...",
                    last_segment.start_offset,
                    self.partition_id
                );
                last_segment
                    .persist_messages(self.storage.segment.clone())
                    .await?;
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
    use crate::streaming::storage::tests::get_test_system_storage;
    use bytes::Bytes;
    use iggy::models::messages::MessageState;
    use iggy::utils::checksum;

    #[tokio::test]
    async fn given_disabled_message_deduplication_all_messages_should_be_appended() {
        let mut partition = create_partition(false);
        let messages = create_messages();
        let messages_count = messages.len() as u32;
        partition.append_messages(messages).await.unwrap();

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
        partition.append_messages(messages).await.unwrap();

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

    fn create_messages() -> Vec<Message> {
        vec![
            create_message(0, 1, "message 1"),
            create_message(1, 2, "message 2"),
            create_message(2, 3, "message 3"),
            create_message(3, 2, "message 3.2"),
            create_message(4, 1, "message 1.2"),
            create_message(5, 3, "message 3.3"),
        ]
    }

    fn create_message(offset: u64, id: u128, payload: &str) -> Message {
        let payload = Bytes::from(payload.to_string());
        let checksum = checksum::calculate(payload.as_ref());
        Message::create(
            offset,
            MessageState::Available,
            1,
            id,
            payload,
            checksum,
            None,
        )
    }
}
