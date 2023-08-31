use crate::partitions::partition::Partition;
use crate::polling_consumer::PollingConsumer;
use crate::segments::segment::Segment;
use crate::utils::random_id;
use iggy::error::Error;
use iggy::models::messages::Message;
use ringbuffer::RingBuffer;
use std::sync::Arc;
use tracing::{error, trace, warn};

const EMPTY_MESSAGES: Vec<Arc<Message>> = vec![];

impl Partition {
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

        let consumer_offset = consumer_offsets.offsets.get(&consumer_id);
        if consumer_offset.is_none() {
            trace!(
                "Consumer: {} hasn't stored offset for partition: {}, returning the first messages...",
                consumer_id,
                self.partition_id
            );
            return self.get_first_messages(count).await;
        }

        let consumer_offset = consumer_offset.unwrap().read().await;
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
        let messages = self.messages.as_ref()?;
        if messages.is_empty() || start_offset > end_offset || end_offset > self.current_offset {
            return None;
        }

        let first_buffered_offset = messages[0].offset;
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

    fn load_messages_from_cache(&self, start_offset: u64, end_offset: u64) -> Vec<Arc<Message>> {
        trace!(
            "Loading messages from cache, start offset: {}, end offset: {}...",
            start_offset,
            end_offset
        );

        if self.messages.is_none() || start_offset > end_offset {
            return EMPTY_MESSAGES;
        }

        let partition_messages = self.messages.as_ref().unwrap();
        if partition_messages.is_empty() {
            return EMPTY_MESSAGES;
        }

        let messages_count = (1 + end_offset - start_offset) as usize;
        let first_offset = partition_messages[0].offset;
        let start_index = start_offset - first_offset;

        let mut messages = Vec::with_capacity(messages_count);
        for i in start_index..start_index + messages_count as u64 {
            let message = partition_messages[i as isize].clone();
            messages.push(message);
        }

        if messages.len() != messages_count {
            error!(
                "Loaded {} messages from cache, expected {}.",
                messages.len(),
                messages_count
            );
        }

        trace!(
            "Loaded {} messages from cache, start offset: {}, end offset: {}...",
            messages.len(),
            start_offset,
            end_offset
        );

        messages
    }

    pub async fn append_messages(&mut self, messages: Vec<Message>) -> Result<(), Error> {
        let segment = self.segments.last_mut();
        if segment.is_none() {
            return Err(Error::SegmentNotFound);
        }

        let mut segment = segment.unwrap();
        if segment.is_closed {
            let start_offset = segment.end_offset + 1;
            trace!(
                "Current segment is closed, creating new segment with start offset: {} for partition with ID: {}...",
                start_offset, self.partition_id
            );
            self.add_persisted_segment(start_offset).await?;
            segment = self.segments.last_mut().unwrap();
        }

        let messages_count = messages.len() as u32;
        trace!(
            "Appending {} messages to segment with start offset: {} for partition with ID: {}...",
            messages_count,
            segment.start_offset,
            self.partition_id
        );

        let deduplicate_messages = self.message_ids.is_some();
        for mut message in messages {
            if message.id == 0 {
                message.id = random_id::get();
            }
            if deduplicate_messages {
                let message_ids = self.message_ids.as_mut().unwrap();
                if message_ids.contains_key(&message.id) {
                    warn!(
                        "Ignored the duplicated message ID: {} for partition with ID: {}.",
                        message.id, self.partition_id
                    );
                    continue;
                }

                message_ids.insert(message.id, true);
            }

            if self.should_increment_offset {
                self.current_offset += 1;
            } else {
                self.should_increment_offset = true;
            }
            trace!(
                "Appending the message with offset: {} to segment with start offset: {} for partition with ID: {}...",
                self.current_offset,
                segment.start_offset,
                self.partition_id
            );

            message.offset = self.current_offset;
            let message = Arc::new(message);
            segment.append_message(message.clone()).await?;
            if self.messages.is_some() {
                self.messages.as_mut().unwrap().push(message);
            }

            trace!(
                "Appended the message with offset: {} to segment with start offset: {} for partition with ID: {}.",
                self.current_offset,
                segment.start_offset,
                self.partition_id
            );
        }

        trace!(
            "Appended {} messages to segment with start offset: {} for partition with ID: {}.",
            messages_count,
            segment.start_offset,
            self.partition_id
        );

        self.unsaved_messages_count += messages_count;
        if self.unsaved_messages_count >= self.config.partition.messages_required_to_save
            || segment.is_full().await
        {
            trace!(
            "Segment with start offset: {} for partition with ID: {} will be persisted on disk...",
            segment.start_offset,
            self.partition_id
        );
            segment
                .persist_messages(self.storage.segment.clone())
                .await?;
            self.unsaved_messages_count = 0;
        }

        Ok(())
    }
}
