use crate::message::Message;
use crate::segments::segment::Segment;
use crate::segments::time_index::TimeIndex;
use crate::segments::*;
use shared::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::{error, trace};

const EMPTY_MESSAGES: Vec<Arc<Message>> = vec![];

impl Segment {
    pub async fn get_messages(
        &self,
        mut offset: u64,
        count: u32,
    ) -> Result<Vec<Arc<Message>>, Error> {
        let mut end_offset = offset + (count - 1) as u64;
        if self.is_closed && end_offset > self.end_offset {
            end_offset = self.end_offset;
        }

        if offset < self.start_offset {
            offset = self.start_offset;
        }

        self.load_messages_from_disk(offset, end_offset).await
    }

    async fn load_messages_from_disk(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<Arc<Message>>, Error> {
        trace!(
            "Loading messages from disk, segment start offset: {}, end offset: {}...",
            start_offset,
            end_offset
        );
        let mut index_file = Segment::open_file(&self.index_path, false).await;
        let index_range =
            index::load_range(&mut index_file, self.start_offset, start_offset, end_offset).await?;

        let mut offset_buffer = [0; 8];
        let mut timestamp_buffer = [0; 8];
        let mut length_buffer = [0; 4];

        let buffer_size = index_range.end_position - index_range.start_position;
        let mut buffer = vec![0; buffer_size as usize];

        let mut log_file = Segment::open_file(&self.log_path, false).await;
        log_file
            .seek(std::io::SeekFrom::Start(index_range.start_position as u64))
            .await?;
        let read_bytes = log_file.read_exact(&mut buffer).await?;
        if read_bytes == 0 {
            return Ok(EMPTY_MESSAGES);
        }

        let length = buffer.len();
        let mut position = 0;

        let messages_count = (1 + end_offset - start_offset) as usize;
        let mut messages: Vec<Arc<Message>> = Vec::with_capacity(messages_count);
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

        if messages.len() != messages_count {
            error!(
                "Loaded {} messages from disk, expected {}.",
                messages.len(),
                messages_count
            );
        }

        trace!(
            "Loaded {} messages from disk, segment start offset: {}, end offset: {}.",
            messages_count,
            start_offset,
            end_offset
        );

        Ok(messages)
    }

    pub async fn append_message(&mut self, message: Arc<Message>) -> Result<(), Error> {
        if self.is_closed {
            return Err(Error::SegmentClosed(self.start_offset, self.partition_id));
        }

        if self.unsaved_messages.is_none() {
            self.unsaved_messages = Some(Vec::new());
        }

        let relative_offset = (message.offset - self.start_offset) as u32;
        self.time_indexes.push(TimeIndex {
            relative_offset,
            timestamp: message.timestamp,
        });

        self.current_size_bytes += message.get_size_bytes();
        self.current_offset = message.offset;
        self.unsaved_messages.as_mut().unwrap().push(message);

        Ok(())
    }

    pub async fn persist_messages(&mut self) -> Result<(), Error> {
        if self.unsaved_messages.is_none() {
            return Ok(());
        }

        let unsaved_messages = self.unsaved_messages.as_ref().unwrap();
        if unsaved_messages.is_empty() {
            return Ok(());
        }

        trace!(
            "Saving {} messages on disk in segment with start offset: {} for partition with ID: {}...",
            unsaved_messages.len(),
            self.start_offset,
            self.partition_id
        );

        let current_bytes = self.saved_bytes;
        let mut log_file = Segment::open_file(&self.log_path, true).await;
        let mut index_file = Segment::open_file(&self.index_path, true).await;
        let mut time_index_file = Segment::open_file(&self.time_index_path, true).await;

        let saved_bytes = log::persist(&mut log_file, unsaved_messages).await?;
        index::persist(&mut index_file, current_bytes, unsaved_messages).await?;
        time_index::persist(&mut time_index_file, unsaved_messages).await?;
        self.saved_bytes += saved_bytes;

        trace!(
            "Saved {} messages on disk in segment with start offset: {} for partition with ID: {}, total bytes written: {}.",
            unsaved_messages.len(),
            self.start_offset,
            self.partition_id,
            saved_bytes
        );

        if self.is_full() {
            self.end_offset = self.current_offset;
            self.is_closed = true;
            self.unsaved_messages = None;
        } else {
            self.unsaved_messages.as_mut().unwrap().clear();
        }

        Ok(())
    }
}
