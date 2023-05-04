use crate::message::Message;
use crate::partitions::partition::Partition;
use crate::segments::segment::Segment;
use crate::timestamp;
use ringbuffer::{RingBuffer, RingBufferExt, RingBufferWrite};
use shared::error::Error;
use std::sync::Arc;
use tracing::{error, trace};

const EMPTY_MESSAGES: Vec<Arc<Message>> = vec![];

impl Partition {
    pub async fn get_messages(&self, offset: u64, count: u32) -> Result<Vec<Arc<Message>>, Error> {
        if self.segments.is_empty() {
            return Ok(EMPTY_MESSAGES);
        }

        let mut end_offset = offset + (count - 1) as u64;
        let segment = self.segments.last().unwrap();
        let max_offset = segment.current_offset;
        if end_offset > max_offset {
            end_offset = max_offset;
        }

        if !self.messages.is_empty() {
            let first_buffered_offset = self.messages[0].offset;
            trace!(
                "First buffered offset: {} for partition: {}",
                first_buffered_offset,
                self.id
            );

            if offset >= first_buffered_offset {
                return self.load_messages_from_cache(offset, end_offset);
            }
        }

        let segments = self
            .segments
            .iter()
            .filter(|segment| {
                (segment.start_offset >= offset && segment.current_offset <= end_offset)
                    || (segment.start_offset <= offset && segment.current_offset >= offset)
                    || (segment.start_offset <= end_offset && segment.current_offset >= end_offset)
            })
            .collect::<Vec<&Segment>>();

        if segments.is_empty() {
            return Ok(EMPTY_MESSAGES);
        }

        if segments.len() == 1 {
            let segment = segments.first().unwrap();
            let messages = segment.get_messages(offset, count).await?;
            return Ok(messages);
        }

        let mut messages = Vec::with_capacity(segments.len());
        for segment in segments {
            let segment_messages = segment.get_messages(offset, count).await?;
            for message in segment_messages {
                messages.push(message);
            }
        }

        Ok(messages)
    }

    fn load_messages_from_cache(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<Arc<Message>>, Error> {
        trace!(
            "Loading messages from cache, start offset: {}, end offset: {}...",
            start_offset,
            end_offset
        );

        let messages_count = (1 + end_offset - start_offset) as usize;
        let messages = self
            .messages
            .iter()
            .filter(|message| message.offset >= start_offset && message.offset <= end_offset)
            .map(Arc::clone)
            .collect::<Vec<Arc<Message>>>();

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

        Ok(messages)
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
                start_offset, self.id
            );
            self.process_new_segment(start_offset).await?;
            segment = self.segments.last_mut().unwrap();
        }

        let messages_count = messages.len() as u32;
        trace!(
            "Appending {} messages to segment with start offset: {} for partition with ID: {}...",
            messages_count,
            segment.start_offset,
            self.id
        );

        for mut message in messages {
            if self.should_increment_offset {
                self.current_offset += 1;
            } else {
                self.should_increment_offset = true;
            }
            trace!(
                "Appending the message with offset: {} to segment with start offset: {} for partition with ID: {}...",
                self.current_offset,
                segment.start_offset,
                self.id
            );

            message.offset = self.current_offset;
            message.timestamp = timestamp::get();
            let message = Arc::new(message);
            segment.append_message(message.clone()).await?;
            self.messages.push(message);

            trace!(
                "Appended the message with offset: {} to segment with start offset: {} for partition with ID: {}.",
                self.current_offset,
                segment.start_offset,
                self.id
            );
        }

        trace!(
            "Appended {} messages to segment with start offset: {} for partition with ID: {}.",
            messages_count,
            segment.start_offset,
            self.id
        );

        self.unsaved_messages_count += messages_count;
        if self.unsaved_messages_count >= self.config.messages_required_to_save || segment.is_full()
        {
            trace!(
            "Segment with start offset: {} for partition with ID: {} will be persisted on disk...",
            segment.start_offset,
            self.id
        );
            segment.persist_messages().await?;
            self.unsaved_messages_count = 0;
        }

        Ok(())
    }

    async fn process_new_segment(&mut self, start_offset: u64) -> Result<(), Error> {
        trace!(
            "Current segment is full, creating new segment for partition with ID: {}",
            self.id
        );
        let mut new_segment = Segment::create(
            self.id,
            start_offset,
            &self.path,
            self.config.segment.clone(),
        );
        new_segment.persist().await?;
        self.segments.push(new_segment);

        Ok(())
    }
}
