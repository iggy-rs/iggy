use crate::message::Message;
use crate::segments::segment::Segment;
use ringbuffer::{AllocRingBuffer, RingBufferWrite};
use shared::error::Error;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tracing::info;

impl Segment {
    pub async fn load(&mut self) -> Result<(), Error> {
        info!(
            "Loading segment from disk for offset: {} and partition with ID: {}...",
            self.start_offset, self.partition_id
        );
        let mut messages = AllocRingBuffer::with_capacity(self.config.messages_buffer as usize);
        let log_file = Segment::open_file(&self.log_path, false).await;
        let file_size = log_file.metadata().await.unwrap().len() as u32;

        info!(
            "Loading messages from segment log file for start offset: {} and partition ID: {}...",
            self.start_offset, self.partition_id
        );

        let mut reader = BufReader::new(log_file);
        loop {
            let offset = reader.read_u64_le().await;
            if offset.is_err() {
                break;
            }

            let timestamp = reader.read_u64_le().await;
            if timestamp.is_err() {
                return Err(Error::CannotReadMessageTimestamp);
            }

            let length = reader.read_u32_le().await;
            if length.is_err() {
                return Err(Error::CannotReadMessageLength);
            }

            let mut payload = vec![0; length.unwrap() as usize];
            if reader.read_exact(&mut payload).await.is_err() {
                return Err(Error::CannotReadMessagePayload);
            }

            let offset = offset.unwrap();
            let message = Message::create(offset, timestamp.unwrap(), payload);
            messages.push(Arc::new(message));
            self.current_offset = offset;
            self.next_saved_message_index += 1;
        }

        self.messages = messages;
        self.should_increment_offset = self.current_offset > 0;
        self.current_size_bytes = file_size;
        self.saved_bytes = self.current_size_bytes;

        info!(
            "Loaded {} bytes from segment log file with start offset {}, current offset: {}, and partition ID: {}.",
            self.current_size_bytes, self.start_offset, self.current_offset, self.partition_id
        );

        Ok(())
    }

    pub async fn persist(&mut self) -> Result<(), Error> {
        info!("Saving segment with start offset: {}", self.start_offset);
        if File::create(&self.log_path).await.is_err() {
            return Err(Error::CannotCreatePartitionSegmentLogFile(
                self.log_path.clone(),
            ));
        }

        if File::create(&self.time_index_path).await.is_err() {
            return Err(Error::CannotCreatePartitionSegmentTimeIndexFile(
                self.log_path.clone(),
            ));
        }

        let index_file = File::create(&self.index_path).await;
        if index_file.is_err() {
            return Err(Error::CannotCreatePartitionSegmentIndexFile(
                self.log_path.clone(),
            ));
        }

        let mut index_file = index_file.unwrap();
        let zero_index = 0u64.to_le_bytes();
        if index_file.write_all(&zero_index).await.is_err() {
            return Err(Error::CannotSaveIndexToSegment);
        }

        info!(
            "Created partition segment log file for start offset: {} and partition with ID: {} and path: {}.",
            self.start_offset, self.partition_id, self.log_path
        );

        Ok(())
    }

    pub async fn open_file(path: &str, append: bool) -> File {
        OpenOptions::new()
            .read(true)
            .append(append)
            .open(path)
            .await
            .unwrap()
    }
}
