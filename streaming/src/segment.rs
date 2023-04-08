use crate::message::Message;
use crate::stream_error::StreamError;
use crate::timestamp;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::info;

pub const LOG_EXTENSION: &str = "log";
pub const INDEX_EXTENSION: &str = "index";
pub const TIME_INDEX_EXTENSION: &str = "timeindex";
pub const SEGMENT_SIZE_BYTES: u64 = 50;
pub const MESSAGES_COUNT_REQUIRED_TO_SAVE: u64 = 4;

#[derive(Debug)]
pub struct Segment {
    pub partition_id: u32,
    pub start_offset: u64,
    pub current_offset: u64,
    pub end_offset: u64,
    pub partition_path: String,
    pub index_path: String,
    pub log_path: String,
    pub timeindex_path: String,
    pub messages: Vec<Message>,
    pub unsaved_messages_count: u64,
    pub size_bytes: u64,
    pub should_increment_offset: bool,
}

impl Segment {
    pub fn create(partition_id: u32, start_offset: u64, partition_path: &str) -> Segment {
        let index_path = format!(
            "{}/{:0>20}.{}",
            partition_path, start_offset, INDEX_EXTENSION
        );
        let timeindex_path = format!(
            "{}/{:0>20}.{}",
            partition_path, start_offset, TIME_INDEX_EXTENSION
        );
        let log_path = format!("{}/{:0>20}.{}", partition_path, start_offset, LOG_EXTENSION);

        Segment {
            partition_id,
            start_offset,
            current_offset: start_offset,
            end_offset: 0,
            partition_path: partition_path.to_string(),
            index_path,
            timeindex_path,
            log_path,
            messages: vec![],
            unsaved_messages_count: 0,
            size_bytes: 0,
            should_increment_offset: false,
        }
    }

    pub fn is_full(&self) -> bool {
        self.size_bytes >= SEGMENT_SIZE_BYTES
    }

    // TODO: Load messages from cache and if not found, load them from disk.
    pub fn get_messages(&self, offset: u64, count: u32) -> Option<Vec<&Message>> {
        let mut end_offset: u64;
        if self.is_full() {
            end_offset = offset + (count - 1) as u64;
            if end_offset > self.end_offset {
                end_offset = self.end_offset;
            }
        } else {
            end_offset = self.current_offset;
        }

        let messages = self
            .messages
            .iter()
            .filter(|message| message.offset >= offset && message.offset <= end_offset)
            .collect::<Vec<&Message>>();

        if messages.is_empty() {
            return None;
        }

        Some(messages)
    }

    pub async fn save_on_disk(&self) -> Result<(), StreamError> {
        if File::create(&self.log_path).await.is_err() {
            return Err(StreamError::CannotCreatePartitionSegmentLogFile(
                self.log_path.clone(),
            ));
        }

        if File::create(&self.index_path).await.is_err() {
            return Err(StreamError::CannotCreatePartitionSegmentIndexFile(
                self.log_path.clone(),
            ));
        }

        if File::create(&self.timeindex_path).await.is_err() {
            return Err(StreamError::CannotCreatePartitionSegmentTimeIndexFile(
                self.log_path.clone(),
            ));
        }

        info!(
            "Created partition segment log file for offset: {} and partition with ID: {} and path: {}.",
            self.start_offset, self.partition_id, self.partition_path
        );
        Ok(())
    }

    pub async fn append_message(&mut self, mut message: Message) -> Result<(), StreamError> {
        if self.is_full() {
            return Err(StreamError::SegmentFull(
                self.start_offset,
                self.partition_id,
            ));
        }

        info!(
            "Appending the message, current segment size is {} bytes",
            self.size_bytes
        );

        // Do not increment offset for the very first message
        if self.should_increment_offset {
            self.current_offset += 1;
        } else {
            self.should_increment_offset = true;
        }

        message.offset = self.current_offset;
        message.timestamp = timestamp::get();
        self.size_bytes += message.get_size_bytes();
        self.messages.push(message);
        self.unsaved_messages_count += 1;

        info!(
            "Appended the message, current segment size is {} bytes",
            self.size_bytes
        );

        if self.unsaved_messages_count >= MESSAGES_COUNT_REQUIRED_TO_SAVE
            || self.size_bytes >= SEGMENT_SIZE_BYTES
        {
            self.save_messages_on_disk().await?;
        }

        if self.is_full() {
            self.end_offset = self.current_offset;
        }

        Ok(())
    }

    pub async fn save_messages_on_disk(&mut self) -> Result<(), StreamError> {
        let log_file = OpenOptions::new().append(true).open(&self.log_path).await;
        if log_file.is_err() {
            return Err(StreamError::LogFileNotFound);
        }

        if self.unsaved_messages_count == 0 {
            if !self.is_full() {
                info!(
                    "No existing messages to save on disk in segment {} for partition {}",
                    self.start_offset, self.partition_id
                );
            }
            return Ok(());
        }

        info!(
            "Saving {} messages on disk in segment {} for partition {}",
            self.unsaved_messages_count, self.start_offset, self.partition_id
        );
        let mut log_file = log_file.unwrap();
        let messages_count = self.messages.len();
        let payload = &self.messages
            [messages_count - self.unsaved_messages_count as usize..messages_count]
            .iter()
            .map(|message| {
                let payload = message.body.as_slice();
                let offset = &message.offset.to_le_bytes();
                let timestamp = &message.timestamp.to_le_bytes();
                let length = &(payload.len() as u64).to_le_bytes();
                [offset, timestamp, length, payload].concat()
            })
            .collect::<Vec<Vec<u8>>>()
            .concat();

        if log_file.write_all(payload).await.is_err() {
            return Err(StreamError::CannotSaveMessagesToSegment);
        }

        info!(
            "Saved {} messages on disk in segment {} for partition {}, total bytes written: {}",
            self.unsaved_messages_count,
            self.start_offset,
            self.partition_id,
            payload.len()
        );

        self.unsaved_messages_count = 0;
        Ok(())
    }

    pub async fn load_from_disk(
        partition_id: u32,
        start_offset: u64,
        partition_path: &str,
    ) -> Result<Segment, StreamError> {
        info!(
            "Loading segment from disk for offset: {} and partition with ID: {}...",
            start_offset, partition_id
        );
        let mut segment = Segment::create(partition_id, start_offset, partition_path);
        let log_file = OpenOptions::new().read(true).open(&segment.log_path).await;

        if log_file.is_err() {
            return Err(StreamError::LogFileNotFound);
        }

        let mut messages = vec![];
        let mut log_file = log_file.unwrap();
        let mut offset_buffer = [0; 8];
        let mut timestamp_buffer = [0; 8];
        let mut length_buffer = [0; 8];

        info!(
            "Loading messages from segment log file for start offset: {} and partition ID: {}...",
            start_offset, partition_id
        );
        while log_file.read_exact(&mut offset_buffer).await.is_ok() {
            if log_file.read_exact(&mut timestamp_buffer).await.is_err() {
                return Err(StreamError::CannotReadMessageTimestamp);
            }

            if log_file.read_exact(&mut length_buffer).await.is_err() {
                return Err(StreamError::CannotReadMessageLength);
            }

            let length = u64::from_le_bytes(length_buffer);
            let mut payload = vec![0; length as usize];
            if log_file.read_exact(&mut payload).await.is_err() {
                return Err(StreamError::CannotReadMessagePayload);
            }

            let offset = u64::from_le_bytes(offset_buffer);
            let message = Message {
                offset,
                timestamp: u64::from_le_bytes(timestamp_buffer),
                body: payload,
            };

            messages.push(message);
            segment.current_offset = offset;
        }

        segment.messages = messages;
        segment.should_increment_offset = segment.current_offset > 0;
        segment.size_bytes = log_file.metadata().await.unwrap().len();

        info!(
            "Loaded {} bytes from segment log file with start offset {} and partition ID: {}.",
            segment.size_bytes, segment.start_offset, partition_id
        );

        Ok(segment)
    }
}
