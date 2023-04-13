use crate::config::SegmentConfig;
use crate::message::Message;
use crate::segments::segment::Segment;
use crate::stream_error::StreamError;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::info;

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
    let mut segment = Segment::create(partition_id, start_offset, partition_path, config.clone());
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

impl Segment {
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

    pub async fn set_files_in_append_mode(&mut self) {
        self.set_files_in_mode(true).await;
    }

    pub async fn set_files_in_read_only_mode(&mut self) {
        self.set_files_in_mode(false).await;
    }

    async fn set_files_in_mode(&mut self, append: bool) {
        self.log_file = Some(Segment::open_file(&self.log_path, append).await);
        self.index_file = Some(Segment::open_file(&self.index_path, append).await);
        self.timeindex_file = Some(Segment::open_file(&self.timeindex_path, append).await);
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
