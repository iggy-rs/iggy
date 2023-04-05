use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info};
use crate::message::Message;
use crate::stream_error::StreamError;
use crate::timestamp;

pub const LOG_EXTENSION: &str = "log";
pub const INDEX_EXTENSION: &str = "index";
pub const TIME_INDEX_EXTENSION: &str = "timeindex";
pub const SEGMENT_SIZE: u64 = 10;

#[derive(Debug)]
pub struct Segment {
    pub start_offset: u64,
    pub current_offset: u64,
    pub end_offset: u64,
    pub partition_path: String,
    pub index_path: String,
    pub log_path: String,
    pub timeindex_path: String,
    pub messages: Vec<Message>
}

impl Segment {
    pub fn create(start_offset: u64, size: u64, partition_path: &str) -> Segment {
        let index_path = format!("{}/{:0>20}.{}", partition_path, start_offset, INDEX_EXTENSION);
        let timeindex_path = format!("{}/{:0>20}.{}", partition_path, start_offset, TIME_INDEX_EXTENSION);
        let log_path = format!("{}/{:0>20}.{}", partition_path, start_offset, LOG_EXTENSION);

        Segment {
            start_offset,
            current_offset: start_offset,
            end_offset: start_offset + size - 1,
            partition_path: partition_path.to_string(),
            index_path,
            timeindex_path,
            log_path,
            messages: vec![]
        }
    }

    // TODO: Load messages from cache and if not found, load them from disk.
    pub fn get_messages(&self, offset: u64, count: u32) -> Option<Vec<&Message>> {
        let mut end_offset = offset + (count - 1) as u64;
        if end_offset > self.end_offset {
            end_offset = self.end_offset;
        }

        let messages = self.messages.iter()
            .filter(|message| message.offset >= offset && message.offset <= end_offset)
            .collect::<Vec<&Message>>();

        if messages.is_empty() {
            return None;
        }

        Some(messages)
    }

    pub async fn save_on_disk(&self) -> Result<(), StreamError> {
        if File::create(&self.log_path).await.is_err() {
            error!("Failed to create partition segment log file for path {}.", self.log_path);
            return Err(StreamError::CannotCreatePartitionLogFile);
        }

        if File::create(&self.index_path).await.is_err() {
            error!("Failed to create partition segment index file for path {}.", self.log_path);
            return Err(StreamError::CannotCreatePartitionIndexFile);
        }

        if File::create(&self.timeindex_path).await.is_err() {
            error!("Failed to create partition segment time index file for path {}.", self.log_path);
            return Err(StreamError::CannotCreatePartitionTimeIndexFile);
        }

        info!("Created partition segment log file for partition path {}.", self.partition_path);

        Ok(())
    }

    pub fn is_full(&self) -> bool {
        self.current_offset == self.end_offset
    }

    //TODO: Make it more efficient by not opening the file for each message
    pub async fn append_message(&mut self, mut message: Message) -> Result<(), StreamError> {
        let log_file = OpenOptions::new()
            .append(true)
            .open(&self.log_path)
            .await;

        if log_file.is_err() {
            return Err(StreamError::LogFileNotFound);
        }

        if self.is_full() {
            return Err(StreamError::SegmentFull);
        }

        if !self.messages.is_empty() {
            self.current_offset += 1;
        }

        message.offset = self.current_offset;
        message.timestamp = timestamp::get();

        let payload = message.body.as_slice();
        let offset = &message.offset.to_le_bytes();
        let timestamp= &message.timestamp.to_le_bytes();
        let length = &(payload.len() as u64).to_le_bytes();
        let data = [offset, timestamp, length, payload].concat();

        self.messages.push(message);

        if log_file.unwrap().write_all(data.as_slice()).await.is_err() {
            return Err(StreamError::CannotAppendMessage);
        }

        Ok(())
    }

    pub async fn load_from_disk(offset: u64, partition_path: &str) -> Result<Segment, StreamError> {
        info!("Loading segment from disk for offset {} and partition path {}...", offset, partition_path);
        let mut segment = Segment::create(offset, SEGMENT_SIZE, partition_path);
        let log_file = OpenOptions::new()
            .read(true)
            .open(&segment.log_path)
            .await;

        if log_file.is_err() {
            return Err(StreamError::LogFileNotFound);
        }

        let mut messages = vec![];
        let mut log_file = log_file.unwrap();
        let mut offset_buffer = [0; 8];
        let mut timestamp_buffer = [0; 8];
        let mut length_buffer = [0; 8];

        info!("Loading messages from segment log file for offset {} and partition path: {}...", offset, partition_path);
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
                body: payload
            };

            messages.push(message);
            segment.current_offset = offset;
        }

        segment.messages = messages;
        info!("Loaded {} messages from segment log file for offsets {}...{} and partition path: {}.", segment.messages.len(), segment.start_offset, segment.end_offset, partition_path);

        Ok(segment)
    }
}