use crate::message::Message;
use crate::segments::segment::Segment;
use crate::segments::*;
use crate::timestamp;
use ringbuffer::{RingBuffer, RingBufferExt, RingBufferWrite};
use shared::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::trace;

const EMPTY_MESSAGES: Vec<Arc<Message>> = vec![];

impl Segment {
    pub async fn get_messages(
        &self,
        mut offset: u64,
        count: u32,
    ) -> Result<Vec<Arc<Message>>, Error> {
        let mut end_offset = offset + count as u64;
        if self.is_full() && end_offset > self.end_offset {
            end_offset = self.end_offset;
        }

        let first_buffered_offset = self.messages[0].offset;
        trace!(
            "First buffered offset: {} for segment: {}",
            first_buffered_offset,
            self.start_offset
        );

        if offset < self.start_offset {
            offset = self.start_offset;
        }

        if offset >= first_buffered_offset {
            return self.load_messages_from_cache(offset, end_offset);
        }

        self.load_messages_from_disk(offset, end_offset).await
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
        let messages = self
            .messages
            .iter()
            .filter(|message| message.offset >= start_offset && message.offset <= end_offset)
            .map(Arc::clone)
            .collect();
        Ok(messages)
    }

    async fn load_messages_from_disk(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<Arc<Message>>, Error> {
        trace!(
            "Loading messages from disk, start offset: {}, end offset: {}...",
            start_offset,
            end_offset
        );
        let mut log_file = Segment::open_file(&self.log_path, false).await;
        let mut index_file = Segment::open_file(&self.index_path, false).await;
        let index_range =
            index::load_range(&mut index_file, self.start_offset, start_offset, end_offset).await?;

        let mut offset_buffer = [0; 8];
        let mut timestamp_buffer = [0; 8];
        let mut length_buffer = [0; 4];

        let buffer_size = index_range.end_position - index_range.start_position;
        let mut buffer = vec![0; buffer_size as usize];
        log_file
            .seek(std::io::SeekFrom::Start(index_range.start_position as u64))
            .await?;
        let read_bytes = log_file.read_exact(&mut buffer).await?;
        if read_bytes == 0 {
            return Ok(EMPTY_MESSAGES);
        }

        let length = buffer.len();
        let mut position = 0;

        let mut messages: Vec<Arc<Message>> = Vec::new();
        while position < length {
            offset_buffer.copy_from_slice(&buffer[position..position + 8]);
            position += 8;
            timestamp_buffer.copy_from_slice(&buffer[position..position + 8]);
            position += 8;
            length_buffer.copy_from_slice(&buffer[position..position + 4]);
            position += 4;

            let offset = u64::from_le_bytes(offset_buffer);
            let timestamp = u64::from_le_bytes(timestamp_buffer);
            let length = u32::from_le_bytes(length_buffer);
            let mut payload = vec![0; length as usize];
            payload.copy_from_slice(&buffer[position..position + length as usize]);
            messages.push(Arc::new(Message::create(offset, timestamp, payload)));
            position += length as usize;
        }

        Ok(messages)
    }

    pub async fn append_messages(&mut self, messages: Vec<Message>) -> Result<(), Error> {
        if self.is_full() {
            return Err(Error::SegmentFull(self.start_offset, self.partition_id));
        }

        for mut message in messages {
            trace!(
                "Appending the message, current segment size is {} bytes",
                self.current_size_bytes
            );

            // Do not increment offset for the very first message
            if self.should_increment_offset {
                self.current_offset += 1;
            } else {
                self.should_increment_offset = true;
            }

            message.offset = self.current_offset;
            message.timestamp = timestamp::get();
            self.current_size_bytes += message.get_size_bytes();
            self.messages.push(Arc::new(message));
            self.unsaved_messages_count += 1;

            trace!(
                "Appended the message, current segment size is {} bytes",
                self.current_size_bytes
            );
        }

        if self.unsaved_messages_count >= self.config.messages_required_to_save || self.is_full() {
            self.persist_messages().await?;
        }

        if self.is_full() {
            self.end_offset = self.current_offset;
        }

        Ok(())
    }

    pub async fn persist_messages(&mut self) -> Result<(), Error> {
        if self.unsaved_messages_count == 0 {
            if !self.is_full() {
                trace!(
                    "No buffered messages to save on disk in segment {} for partition {}",
                    self.start_offset,
                    self.partition_id
                );
            }
            return Ok(());
        }

        trace!(
            "Saving {} messages on disk in segment {} for partition {}",
            self.unsaved_messages_count,
            self.start_offset,
            self.partition_id
        );

        let mut messages = Vec::with_capacity(self.unsaved_messages_count as usize);

        for i in self.next_saved_message_index
            ..self.next_saved_message_index + self.unsaved_messages_count
        {
            let message = &self.messages[i as isize];
            messages.push(message);
            self.next_saved_message_index = i;
        }

        let buffer_capacity = self.messages.capacity() as u32;
        if self.unsaved_messages_count > buffer_capacity {
            self.next_saved_message_index = buffer_capacity - 1;
        } else if self.next_saved_message_index >= buffer_capacity - 1 {
            self.next_saved_message_index = buffer_capacity - self.unsaved_messages_count;
        } else {
            self.next_saved_message_index += 1;
        }

        let current_bytes = self.saved_bytes;
        let mut log_file = Segment::open_file(&self.log_path, true).await;
        let mut index_file = Segment::open_file(&self.index_path, true).await;
        let mut time_index_file = Segment::open_file(&self.time_index_path, true).await;
        let saved_bytes = log::persist(&mut log_file, &messages).await?;
        index::persist(&mut index_file, current_bytes, &messages).await?;
        time_index::persist(&mut time_index_file, &messages).await?;

        trace!(
            "Saved {} messages on disk in segment {} for partition {}, total bytes written: {}",
            self.unsaved_messages_count,
            self.start_offset,
            self.partition_id,
            saved_bytes
        );

        self.unsaved_messages_count = 0;
        self.saved_bytes += saved_bytes;

        Ok(())
    }
}
