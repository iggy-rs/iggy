use crate::config::SegmentConfig;
use crate::message::Message;
use crate::stream_error::StreamError;
use crate::timestamp;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::info;

pub const LOG_EXTENSION: &str = "log";
pub const INDEX_EXTENSION: &str = "index";
pub const TIME_INDEX_EXTENSION: &str = "timeindex";

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
    pub current_size_bytes: u64,
    pub saved_bytes: u64,
    pub should_increment_offset: bool,
    log_file: Option<File>,
    index_file: Option<File>,
    timeindex_file: Option<File>,
    config: Arc<SegmentConfig>,
}

impl Segment {
    pub fn create(
        partition_id: u32,
        start_offset: u64,
        partition_path: &str,
        config: Arc<SegmentConfig>,
    ) -> Segment {
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
            current_size_bytes: 0,
            saved_bytes: 0,
            should_increment_offset: false,
            log_file: None,
            index_file: None,
            timeindex_file: None,
            config,
        }
    }

    pub fn is_full(&self) -> bool {
        self.current_size_bytes >= self.config.size_bytes
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

    pub async fn save_on_disk(&mut self) -> Result<(), StreamError> {
        if File::create(&self.log_path).await.is_err() {
            return Err(StreamError::CannotCreatePartitionSegmentLogFile(
                self.log_path.clone(),
            ));
        }

        if File::create(&self.timeindex_path).await.is_err() {
            return Err(StreamError::CannotCreatePartitionSegmentTimeIndexFile(
                self.log_path.clone(),
            ));
        }

        let index_file = File::create(&self.index_path).await;
        if index_file.is_err() {
            return Err(StreamError::CannotCreatePartitionSegmentIndexFile(
                self.log_path.clone(),
            ));
        }

        let mut index_file = index_file.unwrap();
        let zero_index = 0u64.to_le_bytes();
        if index_file.write_all(&zero_index).await.is_err() {
            return Err(StreamError::CannotSaveIndexToSegment);
        }

        info!(
            "Created partition segment log file for offset: {} and partition with ID: {} and path: {}.",
            self.start_offset, self.partition_id, self.log_path
        );

        self.set_files_in_append_mode().await;

        Ok(())
    }

    pub async fn append_messages(&mut self, mut message: Message) -> Result<(), StreamError> {
        if self.is_full() {
            return Err(StreamError::SegmentFull(
                self.start_offset,
                self.partition_id,
            ));
        }

        info!(
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
        self.messages.push(message);
        self.unsaved_messages_count += 1;

        info!(
            "Appended the message, current segment size is {} bytes",
            self.current_size_bytes
        );

        if self.unsaved_messages_count >= self.config.messages_required_to_save || self.is_full() {
            self.save_messages_on_disk().await?;
        }

        if self.is_full() {
            self.end_offset = self.current_offset;
            self.set_files_in_read_only_mode().await;
        }

        Ok(())
    }

    pub async fn save_messages_on_disk(&mut self) -> Result<(), StreamError> {
        if self.log_file.is_none() {
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

        let messages_count = self.messages.len();
        let messages =
            &self.messages[messages_count - self.unsaved_messages_count as usize..messages_count];

        let log_file_data = messages
            .iter()
            .map(|message| {
                let payload = message.payload.as_slice();
                let offset = &message.offset.to_le_bytes();
                let timestamp = &message.timestamp.to_le_bytes();
                let length = &message.length.to_le_bytes();
                [offset, timestamp, length, payload].concat()
            })
            .collect::<Vec<Vec<u8>>>()
            .concat();

        let log_file = self.log_file.as_mut().unwrap();
        if log_file.write_all(&log_file_data).await.is_err() {
            return Err(StreamError::CannotSaveMessagesToSegment);
        }

        // TODO: Refactor saving indexes

        let mut current_position = self.saved_bytes;
        let index_file_data = messages.iter().fold(vec![], |mut acc, message| {
            current_position += message.get_size_bytes();
            acc.extend_from_slice(&current_position.to_le_bytes());
            acc
        });

        let time_index_file_data = messages
            .iter()
            .map(|message| message.timestamp.to_le_bytes().to_vec())
            .collect::<Vec<Vec<u8>>>()
            .concat();

        let index_file = self.index_file.as_mut().unwrap();
        if index_file.write_all(&index_file_data).await.is_err() {
            return Err(StreamError::CannotSaveIndexToSegment);
        }

        let time_index_file = self.timeindex_file.as_mut().unwrap();
        if time_index_file
            .write_all(&time_index_file_data)
            .await
            .is_err()
        {
            return Err(StreamError::CannotSaveTimeIndexToSegment);
        }

        let saved_bytes = log_file_data.len() as u64;

        info!(
            "Saved {} messages on disk in segment {} for partition {}, total bytes written: {}",
            self.unsaved_messages_count, self.start_offset, self.partition_id, saved_bytes
        );

        self.unsaved_messages_count = 0;
        self.saved_bytes += saved_bytes;
        if self.is_full() {
            self.set_files_in_read_only_mode().await;
        }

        Ok(())
    }

    pub async fn load_from_disk(
        partition_id: u32,
        start_offset: u64,
        partition_path: &str,
        config: Arc<SegmentConfig>,
    ) -> Result<Segment, StreamError> {
        info!(
            "Loading segment from disk for offset: {} and partition with ID: {}...",
            start_offset, partition_id
        );
        let mut segment =
            Segment::create(partition_id, start_offset, partition_path, config.clone());
        let mut messages = vec![];
        let mut log_file = Segment::open_file(&segment.log_path, false).await;
        let mut index_file = Segment::open_file(&segment.index_path, false).await;
        let mut timeindex_file = Segment::open_file(&segment.timeindex_path, false).await;
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
            let timestamp = u64::from_le_bytes(timestamp_buffer);
            let message = Message::create(offset, timestamp, payload);
            messages.push(message);
            segment.current_offset = offset;
        }

        // TODO: Cleanup and refactor loading indexes from disk

        let index_file_len = index_file.metadata().await.unwrap().len();
        let index_file_buffer = &mut vec![0; index_file_len as usize];
        let _ = index_file.read(index_file_buffer).await.unwrap();
        let indexes = index_file_buffer
            .chunks(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<u64>>();

        info!("Indexes per offset: {:?}", indexes);

        let timeindex_file_len = timeindex_file.metadata().await.unwrap().len();
        let timeindex_file_buffer = &mut vec![0; timeindex_file_len as usize];
        let _ = timeindex_file.read(timeindex_file_buffer).await.unwrap();
        let timestamps = timeindex_file_buffer
            .chunks(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<u64>>();

        info!("Timestamps per offset: {:?}", timestamps);

        segment.messages = messages;
        segment.should_increment_offset = segment.current_offset > 0;
        segment.current_size_bytes = log_file.metadata().await.unwrap().len();
        segment.saved_bytes = segment.current_size_bytes;
        if segment.is_full() {
            segment.set_files_in_read_only_mode().await;
        } else {
            segment.set_files_in_append_mode().await;
        }

        info!(
            "Loaded {} bytes from segment log file with start offset {} and partition ID: {}.",
            segment.current_size_bytes, segment.start_offset, partition_id
        );

        Ok(segment)
    }

    async fn set_files_in_append_mode(&mut self) {
        self.set_files_in_mode(true).await;
    }

    async fn set_files_in_read_only_mode(&mut self) {
        self.set_files_in_mode(false).await;
    }

    async fn set_files_in_mode(&mut self, append: bool) {
        self.log_file = Some(Segment::open_file(&self.log_path, append).await);
        self.index_file = Some(Segment::open_file(&self.index_path, append).await);
        self.timeindex_file = Some(Segment::open_file(&self.timeindex_path, append).await);
    }

    async fn open_file(path: &str, append: bool) -> File {
        OpenOptions::new()
            .read(true)
            .append(append)
            .open(path)
            .await
            .unwrap()
    }
}
